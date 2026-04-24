"""
setup_semantic_layer.py
========================
Loads the UCI Household Electric Power Consumption dataset into DuckDB
and creates a semantic layer of SQL views on top of it.

Usage:
    pip install -r requirements.txt
    python src/setup_semantic_layer.py

The script will:
  1. Load the raw CSV into a staging table (stg_readings)
  2. Create a time dimension view (dim_time)
  3. Create metric views at hourly, daily, and monthly grain

All business definitions live HERE — change them once, propagate everywhere.
"""

import duckdb
import os
import sys
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "energy_semantic.duckdb"
RAW_FILE = DATA_DIR / "household_power_consumption.txt"

# ── Business constants — single source of truth ────────────────────────
TARIFF_EUR_PER_KWH = 0.18        # approximate French residential tariff (EDF Bleu)
PEAK_START_HOUR = 6              # peak hours: 06:00 – 21:59
PEAK_END_HOUR = 21


def run(con, sql, label=""):
    """Execute SQL and print status."""
    con.execute(sql)
    if label:
        print(f"  ✓ {label}")


def main():
    # ── Validate data file ──────────────────────────────────────────────
    if not RAW_FILE.exists():
        print(f"\n✗ File not found: {RAW_FILE}")
        print(f"  Download it from Kaggle and place it in: {DATA_DIR}/")
        print(f"  https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set")
        print(f"\n  Or run:  bash scripts/download_data.sh\n")
        sys.exit(1)

    # Remove old DB so we start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))
    print(f"\n── Building semantic layer in {DB_PATH.name} ──\n")

    # ────────────────────────────────────────────────────────────────────
    # LAYER 1: STAGING — parse raw CSV, clean types, rename columns
    # ────────────────────────────────────────────────────────────────────
    run(con, f"""
        CREATE TABLE stg_readings AS
        SELECT
            -- Parse date + time into a proper timestamp
            strptime(Date || ' ' || Time, '%d/%m/%Y %H:%M:%S')  AS reading_ts,

            -- Rename columns to business-friendly names
            CAST(Global_active_power   AS DOUBLE) AS global_active_power_kw,
            CAST(Global_reactive_power AS DOUBLE) AS global_reactive_power_kw,
            CAST(Voltage               AS DOUBLE) AS voltage_v,
            CAST(Global_intensity      AS DOUBLE) AS current_intensity_a,

            -- Submetering (watt-hours per minute)
            CAST(Sub_metering_1 AS DOUBLE) AS sub_metering_1_wh,   -- kitchen
            CAST(Sub_metering_2 AS DOUBLE) AS sub_metering_2_wh,   -- laundry
            CAST(Sub_metering_3 AS DOUBLE) AS sub_metering_3_wh,   -- climate (water heater + AC)

            -- Derived: unmetered consumption
            -- Formula from the dataset docs:
            --   global_active_power*1000/60 - sub1 - sub2 - sub3
            GREATEST(
                CAST(Global_active_power AS DOUBLE) * 1000.0 / 60.0
                - CAST(Sub_metering_1 AS DOUBLE)
                - CAST(Sub_metering_2 AS DOUBLE)
                - CAST(Sub_metering_3 AS DOUBLE),
                0
            ) AS sub_metering_unmetered_wh

        FROM read_csv_auto(
            '{RAW_FILE}',
            sep = ';',
            header = true,
            nullstr = '?',
            types = {{'Date': 'VARCHAR', 'Time': 'VARCHAR'}}
        )
        WHERE Global_active_power IS NOT NULL   -- drop ~1.25% missing rows
    """, "stg_readings (staging table)")

    row_count = con.execute("SELECT COUNT(*) FROM stg_readings").fetchone()[0]
    print(f"    → {row_count:,} rows loaded\n")

    # ────────────────────────────────────────────────────────────────────
    # LAYER 2: DIMENSIONS — time-based attributes for slicing
    # ────────────────────────────────────────────────────────────────────
    run(con, f"""
        CREATE VIEW dim_time AS
        SELECT DISTINCT
            CAST(reading_ts AS DATE)                        AS date_day,
            EXTRACT(YEAR FROM reading_ts)                   AS year,
            EXTRACT(MONTH FROM reading_ts)                  AS month_num,
            monthname(reading_ts)                           AS month_name,
            EXTRACT(DOW FROM reading_ts)                    AS day_of_week_num,   -- 0=Sun
            dayname(reading_ts)                             AS day_of_week_name,
            EXTRACT(HOUR FROM reading_ts)                   AS hour_of_day,

            -- Business rule: peak vs off-peak
            CASE
                WHEN EXTRACT(HOUR FROM reading_ts)
                     BETWEEN {PEAK_START_HOUR} AND {PEAK_END_HOUR}
                THEN 'Peak'
                ELSE 'Off-Peak'
            END                                             AS peak_period,

            -- Business rule: season (Northern Hemisphere)
            CASE EXTRACT(MONTH FROM reading_ts)
                WHEN 12 THEN 'Winter'  WHEN 1  THEN 'Winter'  WHEN 2  THEN 'Winter'
                WHEN 3  THEN 'Spring'  WHEN 4  THEN 'Spring'  WHEN 5  THEN 'Spring'
                WHEN 6  THEN 'Summer'  WHEN 7  THEN 'Summer'  WHEN 8  THEN 'Summer'
                WHEN 9  THEN 'Autumn'  WHEN 10 THEN 'Autumn'  WHEN 11 THEN 'Autumn'
            END                                             AS season,

            -- Weekend flag
            CASE
                WHEN EXTRACT(DOW FROM reading_ts) IN (0, 6)
                THEN 'Weekend'
                ELSE 'Weekday'
            END                                             AS weekday_type

        FROM stg_readings
    """, "dim_time (time dimension view)")

    # ────────────────────────────────────────────────────────────────────
    # LAYER 3: METRICS — pre-aggregated at hourly, daily, monthly grain
    # ────────────────────────────────────────────────────────────────────

    # ── Hourly metrics ──
    run(con, f"""
        CREATE VIEW metric_hourly AS
        SELECT
            DATE_TRUNC('hour', reading_ts)                  AS hour_ts,
            CAST(reading_ts AS DATE)                        AS date_day,
            EXTRACT(HOUR FROM reading_ts)                   AS hour_of_day,

            -- Zone consumption (Wh)
            SUM(sub_metering_1_wh)                          AS kitchen_wh,
            SUM(sub_metering_2_wh)                          AS laundry_wh,
            SUM(sub_metering_3_wh)                          AS climate_wh,
            SUM(sub_metering_unmetered_wh)                  AS unmetered_wh,

            -- Total consumption (Wh)
            SUM(sub_metering_1_wh)
              + SUM(sub_metering_2_wh)
              + SUM(sub_metering_3_wh)
              + SUM(sub_metering_unmetered_wh)              AS total_consumption_wh,

            -- Estimated cost
            (SUM(sub_metering_1_wh)
              + SUM(sub_metering_2_wh)
              + SUM(sub_metering_3_wh)
              + SUM(sub_metering_unmetered_wh)
            ) / 1000.0 * {TARIFF_EUR_PER_KWH}              AS estimated_cost_eur,

            -- Power quality
            AVG(voltage_v)                                  AS avg_voltage,
            MAX(current_intensity_a)                        AS max_current_a,
            AVG(global_active_power_kw)                     AS avg_active_power_kw,

            -- Data quality
            COUNT(*)                                        AS readings_count

        FROM stg_readings
        GROUP BY 1, 2, 3
    """, "metric_hourly (hourly aggregates)")

    # ── Daily metrics ──
    run(con, f"""
        CREATE VIEW metric_daily AS
        SELECT
            date_day,

            -- Zone consumption (Wh → kWh for daily)
            SUM(kitchen_wh)     / 1000.0                    AS kitchen_kwh,
            SUM(laundry_wh)     / 1000.0                    AS laundry_kwh,
            SUM(climate_wh)     / 1000.0                    AS climate_kwh,
            SUM(unmetered_wh)   / 1000.0                    AS unmetered_kwh,
            SUM(total_consumption_wh) / 1000.0              AS total_consumption_kwh,

            -- Cost
            SUM(estimated_cost_eur)                         AS estimated_cost_eur,

            -- Peak vs off-peak split
            SUM(CASE WHEN hour_of_day BETWEEN {PEAK_START_HOUR} AND {PEAK_END_HOUR}
                     THEN total_consumption_wh ELSE 0 END)
                / 1000.0                                    AS peak_kwh,
            SUM(CASE WHEN hour_of_day NOT BETWEEN {PEAK_START_HOUR} AND {PEAK_END_HOUR}
                     THEN total_consumption_wh ELSE 0 END)
                / 1000.0                                    AS offpeak_kwh,

            -- Power quality
            AVG(avg_voltage)                                AS avg_voltage,
            MAX(max_current_a)                              AS max_current_a,

            -- Data quality
            SUM(readings_count)                             AS readings_count

        FROM metric_hourly
        GROUP BY 1
    """, "metric_daily (daily aggregates)")

    # ── Monthly metrics ──
    run(con, f"""
        CREATE VIEW metric_monthly AS
        SELECT
            DATE_TRUNC('month', date_day)                   AS month_start,
            EXTRACT(YEAR FROM date_day)                     AS year,
            monthname(date_day)                             AS month_name,

            SUM(kitchen_kwh)                                AS kitchen_kwh,
            SUM(laundry_kwh)                                AS laundry_kwh,
            SUM(climate_kwh)                                AS climate_kwh,
            SUM(unmetered_kwh)                              AS unmetered_kwh,
            SUM(total_consumption_kwh)                      AS total_consumption_kwh,
            SUM(estimated_cost_eur)                         AS estimated_cost_eur,
            SUM(peak_kwh)                                   AS peak_kwh,
            SUM(offpeak_kwh)                                AS offpeak_kwh,
            AVG(avg_voltage)                                AS avg_voltage,
            COUNT(*)                                        AS days_with_data

        FROM metric_daily
        GROUP BY 1, 2, 3
    """, "metric_monthly (monthly aggregates)")

    # ────────────────────────────────────────────────────────────────────
    # Done
    # ────────────────────────────────────────────────────────────────────
    print(f"""
── Semantic layer ready! ──

Database: {DB_PATH}

Views created:
  dim_time         — time dimension (season, peak/off-peak, weekday/weekend)
  metric_hourly    — hourly consumption by zone + cost + power quality
  metric_daily     — daily roll-ups with peak/off-peak split
  metric_monthly   — monthly roll-ups

Next step:
  python src/run_sample_queries.py
""")

    con.close()


if __name__ == "__main__":
    main()
