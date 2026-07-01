"""Build DuckDB tables for identifying candidate property purchases.

The source files are Philadelphia parcel GeoJSON files named ``properties-*.json``.
They use EPSG:4326 longitude/latitude coordinates, so this script transforms
parcel geometry to EPSG:2272 (Pennsylvania South, US feet) before distance tests.
Owner matching is intentionally conservative: owner1 and owner2 are trimmed,
uppercased, whitespace-collapsed, punctuation-light normalized, and joined in
source order. No fuzzy matching is performed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb


DB_PATH = Path("property-candidates.duckdb")
SOURCE_GLOB = "properties-*.json"
SOURCE_CRS = "EPSG:4326"
WORKING_CRS = "EPSG:2272"
TOUCH_TOLERANCE_FT = 1.0
CLOSE_DISTANCE_FT = 500.0
OPA_CACHE_DIR = Path("opa-property-cache")
OPA_BATCH_SIZE = 200
OPA_REQUEST_DELAY_SECONDS = 3.0
OPA_SQL_API_URL = "https://phl.carto.com/api/v2/sql"


_RAW_COLUMNS = """
    source_file,
    source_feature_id,
    parcel_id,
    opa_account_num,
    owner_name,
    owner_normalized,
    address_number,
    street_address,
    full_address,
    source_srid,
    working_srid,
    geometry_raw,
    geom
"""


RawRow = tuple[Any, ...]
OpaRow = dict[str, Any]


def main() -> None:
    """Parse command-line arguments and rebuild candidate property tables."""

    parser = argparse.ArgumentParser(
        description="Build property-candidates.duckdb from properties-*.json files."
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="DuckDB output path")
    parser.add_argument(
        "--source-glob",
        default=SOURCE_GLOB,
        help="Glob for input parcel GeoJSON files",
    )
    parser.add_argument(
        "--opa-cache-dir",
        type=Path,
        default=OPA_CACHE_DIR,
        help="Directory for cached OPA API JSON responses",
    )
    parser.add_argument(
        "--refresh-opa-cache",
        action="store_true",
        help="Refetch OPA API batches even when cached JSON exists",
    )
    parser.add_argument(
        "--offline-opa-cache",
        action="store_true",
        help="Use only cached OPA JSON; fail instead of making network calls",
    )
    parser.add_argument(
        "--opa-delay-seconds",
        type=float,
        default=OPA_REQUEST_DELAY_SECONDS,
        help="Seconds to wait between uncached OPA API calls",
    )
    args = parser.parse_args()

    source_files = sorted(Path(".").glob(args.source_glob))
    if not source_files:
        raise FileNotFoundError(f"No source files matched {args.source_glob!r}")

    rows = list(read_source_rows(source_files))
    opa_account_nums = sorted({row[3] for row in rows if row[3]})
    fetch_opa_property_cache(
        opa_account_nums,
        args.opa_cache_dir,
        refresh=args.refresh_opa_cache,
        offline=args.offline_opa_cache,
        delay_seconds=args.opa_delay_seconds,
    )
    opa_rows = read_opa_cache_rows(args.opa_cache_dir)
    rebuild_database(args.db, rows, opa_rows)
    print_summary(args.db)


def read_source_rows(source_files: list[Path]) -> list[RawRow]:
    """Read parcel GeoJSON files into normalized rows ready for DuckDB insertion."""

    rows: list[RawRow] = []
    for source_file in source_files:
        data = json.loads(source_file.read_text())
        source_srid = data.get("crs", {}).get("properties", {}).get("name", SOURCE_CRS)

        for feature in data.get("features", []):
            properties = feature.get("properties") or {}
            parcel_id = first_present(properties, "parcel_id", "parcelid", "objectid")
            opa_account_num = first_present(properties, "brt_id", "tencode")
            address = normalize_spaces(str(properties.get("address") or ""))
            address_number = parse_address_number(address)
            owner_name = join_owner_names(properties.get("owner1"), properties.get("owner2"))
            owner_normalized = normalize_owner(owner_name)
            geometry = feature.get("geometry")

            if not parcel_id or not address:
                continue

            rows.append(
                (
                    source_file.name,
                    str(feature.get("id")) if feature.get("id") is not None else None,
                    str(parcel_id),
                    str(opa_account_num),
                    owner_name,
                    owner_normalized,
                    address_number,
                    address,
                    address,
                    source_srid,
                    WORKING_CRS,
                    json.dumps(geometry) if geometry else None,
                )
            )

    return rows


def fetch_opa_property_cache(
    parcel_numbers: list[str],
    cache_dir: Path,
    *,
    refresh: bool,
    offline: bool,
    delay_seconds: float,
) -> None:
    """Fetch OPA enrichment JSON only for missing batches and cache raw responses."""

    cache_dir.mkdir(parents=True, exist_ok=True)
    fetched_any = False
    cached_batches = 0
    fetched_batches = 0
    for batch_index, batch in enumerate(chunked(parcel_numbers, OPA_BATCH_SIZE), start=1):
        cache_path = opa_cache_path(cache_dir, batch_index, batch)
        if cache_path.exists() and not refresh:
            cached_batches += 1
            continue
        if offline:
            raise FileNotFoundError(
                f"Missing cached OPA batch {cache_path}; rerun without "
                "--offline-opa-cache to fetch it."
            )

        if fetched_any and delay_seconds > 0:
            time.sleep(delay_seconds)

        response = fetch_opa_batch(batch)
        cache_path.write_text(json.dumps(response, indent=2, sort_keys=True))
        fetched_any = True
        fetched_batches += 1

    print(f"OPA cache: {cached_batches} cached batch(es), {fetched_batches} fetched")


def fetch_opa_batch(parcel_numbers: list[str]) -> dict[str, Any]:
    """Call the Carto SQL API for one batch of OPA account numbers."""

    quoted_numbers = ",".join("'" + number.replace("'", "''") + "'" for number in parcel_numbers)
    sql = (
        "select * from opa_properties_public_pde "
        f"where parcel_number IN({quoted_numbers})"
    )
    url = OPA_SQL_API_URL + "?" + urllib.parse.urlencode({"q": sql})
    request = urllib.request.Request(
        url,
        headers={
            "accept": "application/json, text/plain, */*",
            "origin": "https://property.phila.gov",
            "referer": "https://property.phila.gov/",
            "user-agent": "twinfarms-property-candidate-builder/0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def read_opa_cache_rows(cache_dir: Path) -> list[OpaRow]:
    """Load cached OPA response rows, deduplicated by parcel_number."""

    by_parcel_number: dict[str, OpaRow] = {}
    for cache_path in sorted(cache_dir.glob("batch-*.json")):
        response = json.loads(cache_path.read_text())
        for row in response.get("rows", []):
            parcel_number = str(row.get("parcel_number") or "")
            if parcel_number:
                by_parcel_number[parcel_number] = row
    return list(by_parcel_number.values())


def opa_cache_path(cache_dir: Path, batch_index: int, batch: list[str]) -> Path:
    """Return a stable file path for a cached OPA API batch."""

    digest = hashlib.md5("\n".join(batch).encode("utf-8")).hexdigest()[:12]
    return cache_dir / f"batch-{batch_index:04d}-{digest}.json"


def chunked(values: list[str], size: int) -> list[list[str]]:
    """Split values into fixed-size chunks."""

    return [values[index : index + size] for index in range(0, len(values), size)]


def rebuild_database(db_path: Path, rows: list[RawRow], opa_rows: list[OpaRow]) -> None:
    """Destructively rebuild raw and derived candidate tables in DuckDB."""

    created_at = datetime.now(UTC).isoformat(timespec="seconds")
    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        drop_existing_tables(con)
        create_raw_properties(con, rows)
        create_opa_properties(con, opa_rows)
        create_canonical_properties(con)
        create_residential_properties(con)
        create_candidate_properties(con, created_at)
        create_candidate_property_parcels(con)
        create_candidate_residential_livable_area_view(con)
        validate_candidate_tables(con)
    finally:
        con.close()


def drop_existing_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Drop persisted output tables so every run is a full refresh."""

    con.execute("DROP VIEW IF EXISTS candidate_residential_livable_area")
    con.execute("DROP TABLE IF EXISTS candidate_property_parcels")
    con.execute("DROP TABLE IF EXISTS candidate_properties")
    con.execute("DROP TABLE IF EXISTS residential_properties")
    con.execute("DROP TABLE IF EXISTS canonical_properties")
    con.execute("DROP TABLE IF EXISTS opa_properties")
    con.execute("DROP TABLE IF EXISTS raw_properties")


def create_raw_properties(con: duckdb.DuckDBPyConnection, rows: list[RawRow]) -> None:
    """Persist source rows and parse GeoJSON geometry into projected feet."""

    con.execute(
        """
        CREATE TEMP TABLE raw_input (
            source_file VARCHAR,
            source_feature_id VARCHAR,
            parcel_id VARCHAR,
            opa_account_num VARCHAR,
            owner_name VARCHAR,
            owner_normalized VARCHAR,
            address_number VARCHAR,
            street_address VARCHAR,
            full_address VARCHAR,
            source_srid VARCHAR,
            working_srid VARCHAR,
            geometry_raw VARCHAR
        )
        """
    )
    con.executemany(
        "INSERT INTO raw_input VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    con.execute(
        f"""
        CREATE TABLE raw_properties AS
        SELECT
            {_RAW_COLUMNS}
        FROM (
            SELECT
                source_file,
                source_feature_id,
                parcel_id,
                opa_account_num,
                owner_name,
                owner_normalized,
                address_number,
                street_address,
                full_address,
                source_srid,
                working_srid,
                geometry_raw,
                CASE
                    WHEN geometry_raw IS NULL THEN NULL
                    ELSE ST_Transform(
                        ST_GeomFromGeoJSON(geometry_raw),
                        source_srid,
                        working_srid,
                        true
                    )
                END AS geom
            FROM raw_input
        )
        """
    )
    con.execute("DROP TABLE raw_input")


def create_opa_properties(con: duckdb.DuckDBPyConnection, rows: list[OpaRow]) -> None:
    """Persist cached OPA enrichment fields needed for residential zoning filters."""

    con.execute(
        """
        CREATE TABLE opa_properties (
            parcel_number VARCHAR,
            zoning VARCHAR,
            address_std VARCHAR,
            market_value DOUBLE,
            sale_price DOUBLE,
            total_area DOUBLE,
            total_livable_area DOUBLE,
            raw_opa_json JSON
        )
        """
    )
    if not rows:
        return

    con.executemany(
        """
        INSERT INTO opa_properties VALUES (?, ?, ?, ?, ?, ?, ?, ?::JSON)
        """,
        [
            (
                str(row.get("parcel_number") or ""),
                str(row.get("zoning") or ""),
                str(row.get("address_std") or ""),
                row.get("market_value"),
                row.get("sale_price"),
                row.get("total_area"),
                row.get("total_livable_area"),
                json.dumps(row, sort_keys=True),
            )
            for row in rows
            if row.get("parcel_number")
        ],
    )


def create_canonical_properties(con: duckdb.DuckDBPyConnection) -> None:
    """Choose one deterministic source row per parcel for candidate generation."""

    con.execute(
        """
        CREATE TABLE canonical_properties AS
        SELECT * EXCLUDE (row_num)
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY parcel_id
                    ORDER BY source_file, source_feature_id NULLS LAST
                ) AS row_num
            FROM raw_properties
        )
        WHERE row_num = 1
        """
    )


def create_residential_properties(con: duckdb.DuckDBPyConnection) -> None:
    """Keep only parcels whose OPA zoning starts with R for candidate generation."""

    con.execute(
        """
        CREATE TABLE residential_properties AS
        SELECT
            c.*,
            o.zoning,
            o.address_std AS opa_address_std,
            o.market_value,
            o.sale_price,
            o.total_area,
            o.total_livable_area
        FROM canonical_properties c
        JOIN opa_properties o
          ON c.opa_account_num = o.parcel_number
        WHERE upper(trim(o.zoning)) LIKE 'R%'
        """
    )


def create_candidate_properties(con: duckdb.DuckDBPyConnection, created_at: str) -> None:
    """Persist one row per exclusive candidate purchase target."""

    con.execute(
        f"""
        CREATE TABLE candidate_properties AS
        WITH
        hyphen AS (
            SELECT
                'HYPHEN_' || md5(parcel_id) AS candidate_id,
                street_address AS human_id,
                'hyphen_address' AS candidate_type,
                owner_normalized,
                1 AS parcel_count,
                'address_number_contains_hyphen' AS match_reason,
                NULL::DOUBLE AS min_pair_distance,
                geom AS geom_union,
                '{created_at}' AS created_at
            FROM residential_properties
            WHERE address_number LIKE '%-%'
        ),
        pairs AS (
            SELECT
                a.parcel_id AS parcel_id_a,
                b.parcel_id AS parcel_id_b,
                a.owner_normalized,
                a.street_address AS address_a,
                b.street_address AS address_b,
                a.geom AS geom_a,
                b.geom AS geom_b,
                ST_Distance(a.geom, b.geom) AS pair_distance,
                ST_Touches(a.geom, b.geom)
                    OR ST_DWithin(a.geom, b.geom, {TOUCH_TOLERANCE_FT}) AS is_touching
            FROM residential_properties a
            JOIN residential_properties b
              ON a.owner_normalized = b.owner_normalized
             AND a.parcel_id < b.parcel_id
            WHERE a.owner_normalized <> ''
              AND a.geom IS NOT NULL
              AND b.geom IS NOT NULL
              AND ST_DWithin(a.geom, b.geom, {CLOSE_DISTANCE_FT})
        ),
        touching AS (
            SELECT
                'TOUCH_' || md5(parcel_id_a || ':' || parcel_id_b) AS candidate_id,
                CASE
                    WHEN address_a <= address_b THEN address_a || ' | ' || address_b
                    ELSE address_b || ' | ' || address_a
                END AS human_id,
                'touching_same_owner' AS candidate_type,
                owner_normalized,
                2 AS parcel_count,
                'same_owner_touching_or_within_1_ft' AS match_reason,
                pair_distance AS min_pair_distance,
                ST_Union(geom_a, geom_b) AS geom_union,
                '{created_at}' AS created_at
            FROM pairs
            WHERE is_touching
        ),
        close AS (
            SELECT
                'CLOSE_' || md5(parcel_id_a || ':' || parcel_id_b) AS candidate_id,
                CASE
                    WHEN address_a <= address_b THEN address_a || ' | ' || address_b
                    ELSE address_b || ' | ' || address_a
                END AS human_id,
                'close_same_owner' AS candidate_type,
                owner_normalized,
                2 AS parcel_count,
                'same_owner_within_500_ft_excluding_touching' AS match_reason,
                pair_distance AS min_pair_distance,
                ST_Union(geom_a, geom_b) AS geom_union,
                '{created_at}' AS created_at
            FROM pairs
            WHERE NOT is_touching
        )
        SELECT * FROM hyphen
        UNION ALL
        SELECT * FROM touching
        UNION ALL
        SELECT * FROM close
        """
    )


def create_candidate_property_parcels(con: duckdb.DuckDBPyConnection) -> None:
    """Persist deterministic candidate-to-parcel join rows."""

    con.execute(
        f"""
        CREATE TABLE candidate_property_parcels AS
        WITH typed_pairs AS (
            SELECT
                cp.candidate_id,
                p.parcel_id AS parcel_id,
                p.street_address,
                p.owner_normalized
            FROM candidate_properties cp
            JOIN residential_properties p
              ON cp.candidate_type = 'hyphen_address'
             AND cp.candidate_id = 'HYPHEN_' || md5(p.parcel_id)

            UNION ALL

            SELECT
                'TOUCH_' || md5(a.parcel_id || ':' || b.parcel_id) AS candidate_id,
                a.parcel_id,
                a.street_address,
                a.owner_normalized
            FROM residential_properties a
            JOIN residential_properties b
              ON a.owner_normalized = b.owner_normalized
             AND a.parcel_id < b.parcel_id
            WHERE a.owner_normalized <> ''
              AND a.geom IS NOT NULL
              AND b.geom IS NOT NULL
              AND (ST_Touches(a.geom, b.geom)
                   OR ST_DWithin(a.geom, b.geom, {TOUCH_TOLERANCE_FT}))

            UNION ALL

            SELECT
                'TOUCH_' || md5(a.parcel_id || ':' || b.parcel_id) AS candidate_id,
                b.parcel_id,
                b.street_address,
                b.owner_normalized
            FROM residential_properties a
            JOIN residential_properties b
              ON a.owner_normalized = b.owner_normalized
             AND a.parcel_id < b.parcel_id
            WHERE a.owner_normalized <> ''
              AND a.geom IS NOT NULL
              AND b.geom IS NOT NULL
              AND (ST_Touches(a.geom, b.geom)
                   OR ST_DWithin(a.geom, b.geom, {TOUCH_TOLERANCE_FT}))

            UNION ALL

            SELECT
                'CLOSE_' || md5(a.parcel_id || ':' || b.parcel_id) AS candidate_id,
                a.parcel_id,
                a.street_address,
                a.owner_normalized
            FROM residential_properties a
            JOIN residential_properties b
              ON a.owner_normalized = b.owner_normalized
             AND a.parcel_id < b.parcel_id
            WHERE a.owner_normalized <> ''
              AND a.geom IS NOT NULL
              AND b.geom IS NOT NULL
              AND ST_DWithin(a.geom, b.geom, {CLOSE_DISTANCE_FT})
              AND NOT (ST_Touches(a.geom, b.geom)
                       OR ST_DWithin(a.geom, b.geom, {TOUCH_TOLERANCE_FT}))

            UNION ALL

            SELECT
                'CLOSE_' || md5(a.parcel_id || ':' || b.parcel_id) AS candidate_id,
                b.parcel_id,
                b.street_address,
                b.owner_normalized
            FROM residential_properties a
            JOIN residential_properties b
              ON a.owner_normalized = b.owner_normalized
             AND a.parcel_id < b.parcel_id
            WHERE a.owner_normalized <> ''
              AND a.geom IS NOT NULL
              AND b.geom IS NOT NULL
              AND ST_DWithin(a.geom, b.geom, {CLOSE_DISTANCE_FT})
              AND NOT (ST_Touches(a.geom, b.geom)
                       OR ST_DWithin(a.geom, b.geom, {TOUCH_TOLERANCE_FT}))
        )
        SELECT
            tm.candidate_id,
            tm.parcel_id,
            row_number() OVER (
                PARTITION BY tm.candidate_id
                ORDER BY tm.street_address, tm.parcel_id
            ) AS parcel_order,
            tm.street_address,
            tm.owner_normalized
        FROM typed_pairs tm
        JOIN candidate_properties cp USING (candidate_id)
        """
    )


def create_candidate_residential_livable_area_view(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Expose residential candidates with 3k-7k combined livable square feet."""

    con.execute(
        """
        CREATE VIEW candidate_residential_livable_area AS
        SELECT
            cp.candidate_id,
            cp.human_id,
            cp.candidate_type,
            cp.owner_normalized,
            cp.parcel_count,
            cp.match_reason,
            cp.min_pair_distance,
            cp.geom_union,
            cp.created_at,
            SUM(rp.total_livable_area) AS combined_total_livable_area,
            MIN(rp.zoning) AS min_zoning,
            MAX(rp.zoning) AS max_zoning,
            SUM(rp.market_value) AS combined_market_value,
            SUM(rp.total_area) AS combined_total_area
        FROM candidate_properties cp
        JOIN candidate_property_parcels cpp USING (candidate_id)
        JOIN residential_properties rp USING (parcel_id)
        GROUP BY
            cp.candidate_id,
            cp.human_id,
            cp.candidate_type,
            cp.owner_normalized,
            cp.parcel_count,
            cp.match_reason,
            cp.min_pair_distance,
            cp.geom_union,
            cp.created_at
        HAVING SUM(rp.total_livable_area) BETWEEN 3000 AND 7000
        """
    )


def validate_candidate_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Run integrity checks that should be true after each refresh."""

    checks = {
        "duplicate candidate_id values": """
            SELECT COUNT(*) FROM (
                SELECT candidate_id
                FROM candidate_properties
                GROUP BY candidate_id
                HAVING COUNT(*) > 1
            )
        """,
        "join rows without candidate": """
            SELECT COUNT(*)
            FROM candidate_property_parcels cpp
            LEFT JOIN candidate_properties cp USING (candidate_id)
            WHERE cp.candidate_id IS NULL
        """,
        "candidate parcel-count mismatches": """
            SELECT COUNT(*)
            FROM candidate_properties cp
            JOIN (
                SELECT candidate_id, COUNT(*) AS actual_count
                FROM candidate_property_parcels
                GROUP BY candidate_id
            ) cpp USING (candidate_id)
            WHERE cp.parcel_count <> cpp.actual_count
        """,
        "candidates without parcels": """
            SELECT COUNT(*)
            FROM candidate_properties cp
            LEFT JOIN candidate_property_parcels cpp USING (candidate_id)
            WHERE cpp.candidate_id IS NULL
        """,
    }
    failures = [name for name, sql in checks.items() if con.execute(sql).fetchone()[0]]
    if failures:
        raise RuntimeError("Validation failed: " + ", ".join(failures))


def print_summary(db_path: Path) -> None:
    """Print a compact build summary for the caller."""

    con = duckdb.connect(str(db_path))
    try:
        con.execute("LOAD spatial;")
        print(f"Built {db_path}")
        for row in con.execute(
            """
            SELECT candidate_type, COUNT(*)
            FROM candidate_properties
            GROUP BY candidate_type
            ORDER BY candidate_type
            """
        ).fetchall():
            print(f"{row[0]}: {row[1]}")
    finally:
        con.close()


def first_present(properties: dict[str, Any], *keys: str) -> str:
    """Return the first present property value as a string, or an empty string."""

    for key in keys:
        value = properties.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def join_owner_names(owner1: Any, owner2: Any) -> str:
    """Join source owner fields without reordering, preserving conservative matching."""

    owners = [normalize_spaces(str(owner)) for owner in (owner1, owner2) if owner]
    return " | ".join(owner for owner in owners if owner)


def normalize_owner(owner_name: str) -> str:
    """Conservatively normalize owner strings for exact equality matching."""

    owner = owner_name.upper().strip()
    owner = re.sub(r"[.,'\"]", "", owner)
    owner = re.sub(r"\s+", " ", owner)
    return owner


def normalize_spaces(value: str) -> str:
    """Trim a string and collapse repeated whitespace."""

    return re.sub(r"\s+", " ", value).strip()


def parse_address_number(address: str) -> str:
    """Return the leading address token used to detect hyphen-address parcels."""

    match = re.match(r"^(\S+)", address)
    return match.group(1) if match else ""


if __name__ == "__main__":
    main()
