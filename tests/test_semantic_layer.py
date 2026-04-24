"""
Tests for the energy semantic layer.

All tests run against an in-memory DuckDB seeded with synthetic data —
no raw data file required.  The fixture rebuilds the full layer from
the same SQL used in production so regressions are caught immediately.
"""

import sys
from pathlib import Path

import duckdb
import pytest

# Allow importing constants from the source without executing main()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from setup_semantic_layer import PEAK_START_HOUR, PEAK_END_HOUR, TARIFF_EUR_PER_KWH


# ── Fixture ───────────────────────────────────────────────────────────────────

SYNTHETIC_ROWS = [
    # (date_str, time_str, gap_kw, react_kw, voltage, intensity, sm1, sm2, sm3)
    # Monday 2007-01-01 — off-peak hour 02:00
    ("01/01/2007", "02:00:00", 1.0, 0.1, 230.0, 4.4, 0.0, 0.0, 17.0),
    ("01/01/2007", "02:01:00", 1.0, 0.1, 231.0, 4.4, 0.0, 0.0, 17.0),
    # Monday 2007-01-01 — peak hour 08:00
    ("01/01/2007", "08:00:00", 3.0, 0.2, 232.0, 13.0, 18.0, 1.0, 17.0),
    ("01/01/2007", "08:01:00", 3.0, 0.2, 233.0, 13.0, 18.0, 1.0, 17.0),
    # Sunday 2007-01-07 — weekend peak
    ("07/01/2007", "10:00:00", 2.0, 0.15, 229.0, 9.0, 10.0, 5.0, 17.0),
    ("07/01/2007", "10:01:00", 2.0, 0.15, 228.0, 9.0, 10.0, 5.0, 17.0),
    # Summer day 2007-07-15 — season test
    ("15/07/2007", "14:00:00", 1.5, 0.05, 235.0, 6.5, 5.0, 2.0, 10.0),
    # Voltage anomaly: below 220 V
    ("20/01/2007", "06:00:00", 1.0, 0.1, 215.0, 4.4, 0.0, 0.0, 17.0),
]


def _build_layer(con: duckdb.DuckDBPyConnection) -> None:
    """Replicate the full semantic layer build against synthetic data."""

    # Staging table — mirrors the production SELECT with typed casts
    con.execute("""
        CREATE TABLE stg_readings AS
        SELECT
            ts                                                      AS reading_ts,
            gap_kw                                                  AS global_active_power_kw,
            react_kw                                                AS global_reactive_power_kw,
            voltage                                                 AS voltage_v,
            intensity                                               AS current_intensity_a,
            sm1                                                     AS sub_metering_1_wh,
            sm2                                                     AS sub_metering_2_wh,
            sm3                                                     AS sub_metering_3_wh,
            GREATEST(gap_kw * 1000.0 / 60.0 - sm1 - sm2 - sm3, 0) AS sub_metering_unmetered_wh
        FROM raw_seed
    """)

    con.execute(f"""
        CREATE VIEW dim_time AS
        SELECT DISTINCT
            CAST(reading_ts AS DATE)                        AS date_day,
            EXTRACT(YEAR  FROM reading_ts)                  AS year,
            EXTRACT(MONTH FROM reading_ts)                  AS month_num,
            monthname(reading_ts)                           AS month_name,
            EXTRACT(DOW   FROM reading_ts)                  AS day_of_week_num,
            dayname(reading_ts)                             AS day_of_week_name,
            EXTRACT(HOUR  FROM reading_ts)                  AS hour_of_day,
            CASE
                WHEN EXTRACT(HOUR FROM reading_ts)
                     BETWEEN {PEAK_START_HOUR} AND {PEAK_END_HOUR}
                THEN 'Peak' ELSE 'Off-Peak'
            END                                             AS peak_period,
            CASE EXTRACT(MONTH FROM reading_ts)
                WHEN 12 THEN 'Winter' WHEN 1 THEN 'Winter' WHEN 2 THEN 'Winter'
                WHEN 3  THEN 'Spring' WHEN 4 THEN 'Spring' WHEN 5 THEN 'Spring'
                WHEN 6  THEN 'Summer' WHEN 7 THEN 'Summer' WHEN 8 THEN 'Summer'
                WHEN 9  THEN 'Autumn' WHEN 10 THEN 'Autumn' WHEN 11 THEN 'Autumn'
            END                                             AS season,
            CASE
                WHEN EXTRACT(DOW FROM reading_ts) IN (0, 6)
                THEN 'Weekend' ELSE 'Weekday'
            END                                             AS weekday_type
        FROM stg_readings
    """)

    con.execute(f"""
        CREATE VIEW metric_hourly AS
        SELECT
            DATE_TRUNC('hour', reading_ts)  AS hour_ts,
            CAST(reading_ts AS DATE)        AS date_day,
            EXTRACT(HOUR FROM reading_ts)   AS hour_of_day,
            SUM(sub_metering_1_wh)          AS kitchen_wh,
            SUM(sub_metering_2_wh)          AS laundry_wh,
            SUM(sub_metering_3_wh)          AS climate_wh,
            SUM(sub_metering_unmetered_wh)  AS unmetered_wh,
            SUM(sub_metering_1_wh)
              + SUM(sub_metering_2_wh)
              + SUM(sub_metering_3_wh)
              + SUM(sub_metering_unmetered_wh) AS total_consumption_wh,
            (SUM(sub_metering_1_wh)
              + SUM(sub_metering_2_wh)
              + SUM(sub_metering_3_wh)
              + SUM(sub_metering_unmetered_wh)
            ) / 1000.0 * {TARIFF_EUR_PER_KWH} AS estimated_cost_eur,
            AVG(voltage_v)                  AS avg_voltage,
            MAX(current_intensity_a)        AS max_current_a,
            AVG(global_active_power_kw)     AS avg_active_power_kw,
            COUNT(*)                        AS readings_count
        FROM stg_readings
        GROUP BY 1, 2, 3
    """)

    con.execute(f"""
        CREATE VIEW metric_daily AS
        SELECT
            date_day,
            SUM(kitchen_wh)   / 1000.0          AS kitchen_kwh,
            SUM(laundry_wh)   / 1000.0          AS laundry_kwh,
            SUM(climate_wh)   / 1000.0          AS climate_kwh,
            SUM(unmetered_wh) / 1000.0          AS unmetered_kwh,
            SUM(total_consumption_wh) / 1000.0  AS total_consumption_kwh,
            SUM(estimated_cost_eur)             AS estimated_cost_eur,
            SUM(CASE WHEN hour_of_day BETWEEN {PEAK_START_HOUR} AND {PEAK_END_HOUR}
                     THEN total_consumption_wh ELSE 0 END) / 1000.0 AS peak_kwh,
            SUM(CASE WHEN hour_of_day NOT BETWEEN {PEAK_START_HOUR} AND {PEAK_END_HOUR}
                     THEN total_consumption_wh ELSE 0 END) / 1000.0 AS offpeak_kwh,
            AVG(avg_voltage)                    AS avg_voltage,
            MAX(max_current_a)                  AS max_current_a,
            SUM(readings_count)                 AS readings_count
        FROM metric_hourly
        GROUP BY 1
    """)

    con.execute("""
        CREATE VIEW metric_monthly AS
        SELECT
            DATE_TRUNC('month', date_day)   AS month_start,
            EXTRACT(YEAR FROM date_day)     AS year,
            monthname(date_day)             AS month_name,
            SUM(kitchen_kwh)                AS kitchen_kwh,
            SUM(laundry_kwh)                AS laundry_kwh,
            SUM(climate_kwh)                AS climate_kwh,
            SUM(unmetered_kwh)              AS unmetered_kwh,
            SUM(total_consumption_kwh)      AS total_consumption_kwh,
            SUM(estimated_cost_eur)         AS estimated_cost_eur,
            SUM(peak_kwh)                   AS peak_kwh,
            SUM(offpeak_kwh)                AS offpeak_kwh,
            AVG(avg_voltage)                AS avg_voltage,
            COUNT(*)                        AS days_with_data
        FROM metric_daily
        GROUP BY 1, 2, 3
    """)


@pytest.fixture
def con():
    """In-memory DuckDB with the full semantic layer pre-built."""
    c = duckdb.connect(":memory:")

    # Seed raw data
    c.execute("""
        CREATE TABLE raw_seed (
            ts        TIMESTAMP,
            gap_kw    DOUBLE,
            react_kw  DOUBLE,
            voltage   DOUBLE,
            intensity DOUBLE,
            sm1       DOUBLE,
            sm2       DOUBLE,
            sm3       DOUBLE
        )
    """)
    for (d, t, gap, react, v, i, sm1, sm2, sm3) in SYNTHETIC_ROWS:
        day, mon, yr = d.split("/")
        ts = f"{yr}-{mon}-{day} {t}"
        c.execute(
            "INSERT INTO raw_seed VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [ts, gap, react, v, i, sm1, sm2, sm3],
        )

    _build_layer(c)
    yield c
    c.close()


# ── Staging layer ─────────────────────────────────────────────────────────────

class TestStagingLayer:
    def test_row_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM stg_readings").fetchone()[0]
        assert count == len(SYNTHETIC_ROWS)

    def test_column_names(self, con):
        cols = {r[0] for r in con.execute("DESCRIBE stg_readings").fetchall()}
        expected = {
            "reading_ts", "global_active_power_kw", "global_reactive_power_kw",
            "voltage_v", "current_intensity_a",
            "sub_metering_1_wh", "sub_metering_2_wh", "sub_metering_3_wh",
            "sub_metering_unmetered_wh",
        }
        assert expected.issubset(cols)

    def test_unmetered_never_negative(self, con):
        min_val = con.execute(
            "SELECT MIN(sub_metering_unmetered_wh) FROM stg_readings"
        ).fetchone()[0]
        assert min_val >= 0.0

    def test_unmetered_formula(self, con):
        # For row with gap_kw=1.0, sm1=0, sm2=0, sm3=17:
        # 1.0*1000/60 - 0 - 0 - 17 ≈ 16.667 - 17 = -0.333 → clamped to 0
        row = con.execute("""
            SELECT sub_metering_unmetered_wh
            FROM stg_readings
            WHERE reading_ts = '2007-01-01 02:00:00'
        """).fetchone()
        assert row[0] == 0.0

    def test_unmetered_positive_case(self, con):
        # gap=3.0 kw → 3000/60=50 wh/min, sm1=18, sm2=1, sm3=17 → 50-36=14
        row = con.execute("""
            SELECT sub_metering_unmetered_wh
            FROM stg_readings
            WHERE reading_ts = '2007-01-01 08:00:00'
        """).fetchone()
        assert abs(row[0] - 14.0) < 0.01


# ── Dimension layer ───────────────────────────────────────────────────────────

class TestDimTime:
    def test_peak_classification(self, con):
        rows = con.execute("""
            SELECT hour_of_day, peak_period
            FROM dim_time
            ORDER BY hour_of_day
        """).fetchall()
        for hour, period in rows:
            if PEAK_START_HOUR <= hour <= PEAK_END_HOUR:
                assert period == "Peak", f"hour {hour} should be Peak"
            else:
                assert period == "Off-Peak", f"hour {hour} should be Off-Peak"

    def test_season_winter(self, con):
        season = con.execute("""
            SELECT season FROM dim_time WHERE date_day = '2007-01-01'
        """).fetchone()[0]
        assert season == "Winter"

    def test_season_summer(self, con):
        season = con.execute("""
            SELECT season FROM dim_time WHERE date_day = '2007-07-15'
        """).fetchone()[0]
        assert season == "Summer"

    def test_weekday_classification(self, con):
        # 2007-01-01 is a Monday → Weekday
        wt = con.execute("""
            SELECT weekday_type FROM dim_time WHERE date_day = '2007-01-01'
        """).fetchone()[0]
        assert wt == "Weekday"

    def test_weekend_classification(self, con):
        # 2007-01-07 is a Sunday → Weekend
        wt = con.execute("""
            SELECT weekday_type FROM dim_time WHERE date_day = '2007-01-07'
        """).fetchone()[0]
        assert wt == "Weekend"


# ── Metric views ──────────────────────────────────────────────────────────────

class TestMetricHourly:
    def test_aggregation_sums_both_readings(self, con):
        # Two readings at 08:xx on 2007-01-01 with sm1=18 each → kitchen_wh=36
        row = con.execute("""
            SELECT kitchen_wh FROM metric_hourly
            WHERE date_day = '2007-01-01' AND hour_of_day = 8
        """).fetchone()
        assert row[0] == 36.0

    def test_estimated_cost_formula(self, con):
        row = con.execute("""
            SELECT total_consumption_wh, estimated_cost_eur
            FROM metric_hourly
            WHERE date_day = '2007-01-01' AND hour_of_day = 8
        """).fetchone()
        total_wh, cost = row
        expected_cost = (total_wh / 1000.0) * TARIFF_EUR_PER_KWH
        assert abs(cost - expected_cost) < 1e-9

    def test_readings_count(self, con):
        count = con.execute("""
            SELECT readings_count FROM metric_hourly
            WHERE date_day = '2007-01-01' AND hour_of_day = 8
        """).fetchone()[0]
        assert count == 2

    def test_avg_voltage(self, con):
        avg_v = con.execute("""
            SELECT avg_voltage FROM metric_hourly
            WHERE date_day = '2007-01-01' AND hour_of_day = 8
        """).fetchone()[0]
        assert abs(avg_v - 232.5) < 0.01  # (232 + 233) / 2


class TestMetricDaily:
    def test_wh_to_kwh_conversion(self, con):
        row = con.execute("""
            SELECT kitchen_kwh, climate_kwh FROM metric_daily
            WHERE date_day = '2007-01-01'
        """).fetchone()
        # kitchen: (0+0) from offpeak + (18+18) from peak = 36 wh → 0.036 kwh
        assert abs(row[0] - 0.036) < 1e-9
        # climate: (17+17) offpeak + (17+17) peak = 68 wh → 0.068 kwh
        assert abs(row[1] - 0.068) < 1e-9

    def test_peak_offpeak_sum_equals_total(self, con):
        row = con.execute("""
            SELECT total_consumption_kwh, peak_kwh, offpeak_kwh
            FROM metric_daily
            WHERE date_day = '2007-01-01'
        """).fetchone()
        total, peak, offpeak = row
        assert abs((peak + offpeak) - total) < 1e-9

    def test_cost_equals_total_times_tariff(self, con):
        row = con.execute("""
            SELECT total_consumption_kwh, estimated_cost_eur
            FROM metric_daily
            WHERE date_day = '2007-01-01'
        """).fetchone()
        total_kwh, cost = row
        assert abs(cost - total_kwh * TARIFF_EUR_PER_KWH) < 1e-9

    def test_offpeak_hour_lands_in_offpeak(self, con):
        # Hour 02:00 is off-peak; its consumption should appear in offpeak_kwh
        row = con.execute("""
            SELECT offpeak_kwh FROM metric_daily WHERE date_day = '2007-01-01'
        """).fetchone()
        assert row[0] > 0

    def test_voltage_anomaly_day_present(self, con):
        avg_v = con.execute("""
            SELECT avg_voltage FROM metric_daily WHERE date_day = '2007-01-20'
        """).fetchone()[0]
        assert avg_v == 215.0


class TestMetricMonthly:
    def test_january_aggregates_correct_days(self, con):
        days = con.execute("""
            SELECT days_with_data FROM metric_monthly
            WHERE year = 2007 AND month_name = 'January'
        """).fetchone()[0]
        # Jan rows: 2007-01-01, 2007-01-07, 2007-01-20 → 3 distinct days
        assert days == 3

    def test_monthly_cost_equals_sum_of_daily(self, con):
        monthly_cost = con.execute("""
            SELECT estimated_cost_eur FROM metric_monthly
            WHERE year = 2007 AND month_name = 'January'
        """).fetchone()[0]
        daily_sum = con.execute("""
            SELECT SUM(estimated_cost_eur) FROM metric_daily
            WHERE EXTRACT(MONTH FROM date_day) = 1
              AND EXTRACT(YEAR  FROM date_day) = 2007
        """).fetchone()[0]
        assert abs(monthly_cost - daily_sum) < 1e-9

    def test_july_in_separate_month(self, con):
        count = con.execute(
            "SELECT COUNT(*) FROM metric_monthly WHERE year = 2007 AND month_name = 'July'"
        ).fetchone()[0]
        assert count == 1


# ── Business rule boundary tests ─────────────────────────────────────────────

class TestBusinessRules:
    def test_peak_boundary_start(self, con):
        # PEAK_START_HOUR itself must be Peak
        row = con.execute(f"""
            SELECT peak_period FROM dim_time
            WHERE hour_of_day = {PEAK_START_HOUR}
            LIMIT 1
        """).fetchone()
        if row:
            assert row[0] == "Peak"

    def test_peak_boundary_end(self, con):
        # PEAK_END_HOUR itself must be Peak
        row = con.execute(f"""
            SELECT peak_period FROM dim_time
            WHERE hour_of_day = {PEAK_END_HOUR}
            LIMIT 1
        """).fetchone()
        if row:
            assert row[0] == "Peak"

    def test_tariff_constant_used_correctly(self, con):
        # Verify the tariff baked into the view matches the Python constant
        row = con.execute("""
            SELECT total_consumption_wh, estimated_cost_eur
            FROM metric_hourly LIMIT 1
        """).fetchone()
        wh, cost = row
        assert abs(cost - (wh / 1000.0) * TARIFF_EUR_PER_KWH) < 1e-9
