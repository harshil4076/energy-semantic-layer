"""
Build the buildings semantic layer from Building Data Genome Project 2.

Reads 8 wide-format meter CSVs + metadata, unpivots to long format,
and creates layered DuckDB views for analytics.

Usage:
    python src/setup_buildings_layer.py
"""

import os
import duckdb

# ── Business constants ──────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "buildings")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "buildings_semantic.duckdb")

# Meter types to ingest (use _cleaned variants — missing values filled by BDG2)
METER_TYPES = [
    "electricity",
    "gas",
    "hotwater",
    "chilledwater",
    "steam",
    "water",
    "irrigation",
    "solar",
]

# Business hours definition (local time approximation — data is UTC-ish)
BUSINESS_HOURS_START = 8   # 08:00
BUSINESS_HOURS_END = 18    # 17:59


def csv_path(meter_type: str) -> str:
    return os.path.join(DATA_DIR, f"{meter_type}_cleaned.csv")


def build_layer(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    print(f"\n── Building buildings semantic layer in {os.path.basename(db_path)} ──\n")
    con = duckdb.connect(db_path)
    _build(con)
    return con


def _build(con: duckdb.DuckDBPyConnection) -> None:
    # ── Layer 1: staging ────────────────────────────────────────────────────
    _build_stg_meter_readings(con)
    _build_dim_building(con)

    # ── Layer 2: time dimension ─────────────────────────────────────────────
    _build_dim_time(con)

    # ── Layer 3: metrics ────────────────────────────────────────────────────
    _build_metric_building_hourly(con)
    _build_metric_building_daily(con)
    _build_metric_site_monthly(con)


def _build_stg_meter_readings(con: duckdb.DuckDBPyConnection) -> None:
    """
    Unpivot each wide-format meter CSV from (timestamp, building1, building2, ...)
    to (ts, building_id, meter_type, kwh), then union all 8 meter types.
    """
    union_parts = []
    for meter in METER_TYPES:
        path = csv_path(meter)
        if not os.path.exists(path):
            print(f"  ⚠  {meter}_cleaned.csv not found — skipping")
            continue
        union_parts.append(f"""
            SELECT
                ts::TIMESTAMP AS ts,
                building_id,
                '{meter}' AS meter_type,
                TRY_CAST(kwh AS DOUBLE) AS kwh
            FROM (
                UNPIVOT (
                    SELECT timestamp AS ts, * EXCLUDE (timestamp)
                    FROM read_csv_auto('{path}', all_varchar=true)
                )
                ON COLUMNS(* EXCLUDE ts)
                INTO NAME building_id VALUE kwh
            )
            WHERE TRY_CAST(kwh AS DOUBLE) IS NOT NULL
              AND TRY_CAST(kwh AS DOUBLE) >= 0
        """)

    if not union_parts:
        raise RuntimeError("No meter CSV files found — check data/buildings/")

    sql = " UNION ALL ".join(union_parts)
    con.execute("DROP TABLE IF EXISTS stg_meter_readings")
    con.execute(f"CREATE TABLE stg_meter_readings AS {sql}")
    row_count = con.execute("SELECT COUNT(*) FROM stg_meter_readings").fetchone()[0]
    print(f"  ✓ stg_meter_readings  →  {row_count:,} rows loaded")


def _build_dim_building(con: duckdb.DuckDBPyConnection) -> None:
    """Load metadata.csv into dim_building, keeping the columns most useful for analytics."""
    meta_path = os.path.join(DATA_DIR, "metadata.csv")
    con.execute("DROP TABLE IF EXISTS dim_building")
    con.execute(f"""
        CREATE TABLE dim_building AS
        SELECT
            building_id,
            site_id,
            primaryspaceusage,
            sub_primaryspaceusage,
            CAST(sqm AS DOUBLE)  AS sqm,
            CAST(sqft AS DOUBLE) AS sqft,
            CAST(lat AS DOUBLE)  AS lat,
            CAST(lng AS DOUBLE)  AS lng,
            timezone,
            industry,
            CAST(yearbuilt AS INTEGER) AS yearbuilt,
            CAST(numberoffloors AS INTEGER) AS numberoffloors,
            electricity  AS has_electricity,
            gas          AS has_gas,
            hotwater     AS has_hotwater,
            chilledwater AS has_chilledwater,
            steam        AS has_steam,
            solar        AS has_solar
        FROM read_csv_auto('{meta_path}')
    """)
    building_count = con.execute("SELECT COUNT(*) FROM dim_building").fetchone()[0]
    print(f"  ✓ dim_building        →  {building_count:,} buildings")


def _build_dim_time(con: duckdb.DuckDBPyConnection) -> None:
    """One row per unique timestamp from stg_meter_readings, with business attributes."""
    con.execute("DROP VIEW IF EXISTS dim_time")
    con.execute(f"""
        CREATE VIEW dim_time AS
        SELECT
            ts,
            ts::DATE                                  AS date,
            EXTRACT(year  FROM ts)::INTEGER           AS year,
            EXTRACT(month FROM ts)::INTEGER           AS month,
            EXTRACT(day   FROM ts)::INTEGER           AS day,
            EXTRACT(hour  FROM ts)::INTEGER           AS hour,
            EXTRACT(dow   FROM ts)::INTEGER           AS weekday_num,
            CASE EXTRACT(dow FROM ts)::INTEGER
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END                                       AS weekday_name,
            CASE
                WHEN EXTRACT(dow FROM ts) IN (0, 6) THEN 'Weekend'
                ELSE 'Weekday'
            END                                       AS weekday_type,
            CASE EXTRACT(month FROM ts)::INTEGER
                WHEN 12 THEN 'Winter'  WHEN 1 THEN 'Winter'  WHEN 2 THEN 'Winter'
                WHEN 3  THEN 'Spring'  WHEN 4 THEN 'Spring'  WHEN 5 THEN 'Spring'
                WHEN 6  THEN 'Summer'  WHEN 7 THEN 'Summer'  WHEN 8 THEN 'Summer'
                ELSE 'Autumn'
            END                                       AS season,
            CASE
                WHEN EXTRACT(hour FROM ts) BETWEEN {BUSINESS_HOURS_START} AND {BUSINESS_HOURS_END - 1}
                     AND EXTRACT(dow FROM ts) NOT IN (0, 6)
                THEN TRUE
                ELSE FALSE
            END                                       AS is_business_hours
        FROM (SELECT DISTINCT ts FROM stg_meter_readings)
    """)
    print("  ✓ dim_time")


def _build_metric_building_hourly(con: duckdb.DuckDBPyConnection) -> None:
    """
    Hourly consumption per building + meter type, enriched with building metadata.
    EUI column: kWh/sqft for that hour (multiply by 8760 to annualize).
    """
    con.execute("DROP VIEW IF EXISTS metric_building_hourly")
    con.execute("""
        CREATE VIEW metric_building_hourly AS
        SELECT
            s.ts,
            s.building_id,
            s.meter_type,
            s.kwh,
            b.site_id,
            b.primaryspaceusage,
            b.sqm,
            b.sqft,
            -- EUI: kWh per sqft for this hour; annualize by × 8760
            CASE
                WHEN b.sqft > 0 THEN s.kwh / b.sqft
                ELSE NULL
            END AS kwh_per_sqft,
            t.date,
            t.year,
            t.month,
            t.hour,
            t.weekday_type,
            t.season,
            t.is_business_hours
        FROM stg_meter_readings s
        JOIN dim_time     t ON s.ts = t.ts
        LEFT JOIN dim_building b ON s.building_id = b.building_id
    """)
    print("  ✓ metric_building_hourly")


def _build_metric_building_daily(con: duckdb.DuckDBPyConnection) -> None:
    """Daily totals per building + meter type."""
    con.execute("DROP VIEW IF EXISTS metric_building_daily")
    con.execute("""
        CREATE VIEW metric_building_daily AS
        SELECT
            date,
            building_id,
            site_id,
            primaryspaceusage,
            meter_type,
            SUM(kwh)              AS total_kwh,
            COUNT(*)              AS hours_with_data,
            AVG(kwh)              AS avg_hourly_kwh,
            MAX(kwh)              AS peak_hourly_kwh,
            sqft,
            sqm,
            -- Daily EUI: kWh/sqft for this day; multiply by 365 to annualize
            CASE
                WHEN sqft > 0 THEN SUM(kwh) / sqft
                ELSE NULL
            END AS daily_kwh_per_sqft,
            MAX(year)             AS year,
            MAX(month)            AS month,
            MAX(season)           AS season,
            MAX(weekday_type)     AS weekday_type
        FROM metric_building_hourly
        GROUP BY date, building_id, site_id, primaryspaceusage, meter_type, sqft, sqm
    """)
    print("  ✓ metric_building_daily")


def _build_metric_site_monthly(con: duckdb.DuckDBPyConnection) -> None:
    """Monthly rollup per site + meter type — good for site-level benchmarking."""
    con.execute("DROP VIEW IF EXISTS metric_site_monthly")
    con.execute("""
        CREATE VIEW metric_site_monthly AS
        SELECT
            year,
            month,
            site_id,
            meter_type,
            SUM(total_kwh)          AS total_kwh,
            AVG(total_kwh)          AS avg_building_kwh,
            COUNT(DISTINCT building_id) AS building_count,
            -- Site EUI: total kWh divided by total sqft for the month
            CASE
                WHEN SUM(sqft) > 0 THEN SUM(total_kwh) / SUM(sqft)
                ELSE NULL
            END                     AS monthly_kwh_per_sqft
        FROM metric_building_daily
        GROUP BY year, month, site_id, meter_type
    """)
    print("  ✓ metric_site_monthly")
    print()


if __name__ == "__main__":
    build_layer()
    print("Done. Database written to data/buildings_semantic.duckdb\n")
