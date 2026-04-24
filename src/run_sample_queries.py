"""
run_sample_queries.py
======================
Demonstrates querying the semantic layer instead of raw data.

Every query uses plain business terms (kitchen_kwh, season,
estimated_cost_eur). Nobody needs to remember that sub_metering_1
means kitchen or that cost = wh/1000 * 0.18.

Run after:  python src/setup_semantic_layer.py
"""

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "energy_semantic.duckdb"

if not DB_PATH.exists():
    print(f"✗ Database not found at {DB_PATH}")
    print(f"  Run first:  python src/setup_semantic_layer.py")
    raise SystemExit(1)

con = duckdb.connect(str(DB_PATH), read_only=True)


def heading(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Query 1: Monthly consumption by zone ────────────────────────────
heading("1. Monthly total consumption by zone (kWh)")
print(con.sql("""
    SELECT
        year,
        month_name,
        ROUND(kitchen_kwh, 1)     AS kitchen,
        ROUND(laundry_kwh, 1)     AS laundry,
        ROUND(climate_kwh, 1)     AS climate,
        ROUND(unmetered_kwh, 1)   AS unmetered,
        ROUND(total_consumption_kwh, 1) AS total
    FROM metric_monthly
    ORDER BY month_start
    LIMIT 12
"""))


# ── Query 2: Seasonal averages ──────────────────────────────────────
heading("2. Average daily consumption by season (kWh)")
print(con.sql("""
    SELECT
        t.season,
        ROUND(AVG(d.total_consumption_kwh), 2)  AS avg_daily_kwh,
        ROUND(AVG(d.estimated_cost_eur), 2)     AS avg_daily_cost_eur,
        ROUND(AVG(d.peak_kwh), 2)               AS avg_peak_kwh,
        ROUND(AVG(d.offpeak_kwh), 2)            AS avg_offpeak_kwh
    FROM metric_daily d
    JOIN dim_time t ON d.date_day = t.date_day
    GROUP BY t.season
    ORDER BY avg_daily_kwh DESC
"""))


# ── Query 3: Weekday vs Weekend ─────────────────────────────────────
heading("3. Weekday vs Weekend consumption")
print(con.sql("""
    SELECT
        t.weekday_type,
        ROUND(AVG(d.total_consumption_kwh), 2)  AS avg_daily_kwh,
        ROUND(AVG(d.kitchen_kwh), 2)            AS avg_kitchen_kwh,
        ROUND(AVG(d.climate_kwh), 2)            AS avg_climate_kwh,
        COUNT(*)                                AS days_sampled
    FROM metric_daily d
    JOIN dim_time t ON d.date_day = t.date_day
    GROUP BY t.weekday_type
"""))


# ── Query 4: Hourly load profile ────────────────────────────────────
heading("4. Average hourly load profile — which hour is most expensive?")
print(con.sql("""
    SELECT
        hour_of_day,
        ROUND(AVG(avg_active_power_kw), 3)      AS avg_kw,
        ROUND(AVG(estimated_cost_eur), 4)        AS avg_cost_eur,
        ROUND(AVG(kitchen_wh), 1)                AS avg_kitchen_wh,
        ROUND(AVG(climate_wh), 1)                AS avg_climate_wh
    FROM metric_hourly
    GROUP BY hour_of_day
    ORDER BY hour_of_day
"""))


# ── Query 5: Top 10 highest-consumption days ────────────────────────
heading("5. Top 10 highest-consumption days")
print(con.sql("""
    SELECT
        date_day,
        ROUND(total_consumption_kwh, 1) AS total_kwh,
        ROUND(estimated_cost_eur, 2)    AS cost_eur,
        ROUND(peak_kwh, 1)             AS peak_kwh,
        ROUND(offpeak_kwh, 1)          AS offpeak_kwh
    FROM metric_daily
    ORDER BY total_consumption_kwh DESC
    LIMIT 10
"""))


# ── Query 6: Voltage quality check ──────────────────────────────────
heading("6. Days with average voltage outside normal range (220–240 V)")
print(con.sql("""
    SELECT
        date_day,
        ROUND(avg_voltage, 1)           AS avg_voltage,
        ROUND(total_consumption_kwh, 1) AS total_kwh
    FROM metric_daily
    WHERE avg_voltage < 220 OR avg_voltage > 240
    ORDER BY avg_voltage
    LIMIT 10
"""))


# ── Query 7: Year-over-year comparison ──────────────────────────────
heading("7. Year-over-year total consumption and cost")
print(con.sql("""
    SELECT
        EXTRACT(YEAR FROM date_day)          AS year,
        ROUND(SUM(total_consumption_kwh), 0) AS total_kwh,
        ROUND(SUM(estimated_cost_eur), 2)    AS total_cost_eur,
        COUNT(*)                             AS days_with_data
    FROM metric_daily
    GROUP BY 1
    ORDER BY 1
"""))


print(f"\n{'─' * 60}")
print("  All queries used the semantic layer — no raw column names!")
print(f"{'─' * 60}\n")

con.close()
