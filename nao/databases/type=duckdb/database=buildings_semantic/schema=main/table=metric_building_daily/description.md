# metric_building_daily

**Dataset:** `main`

## Description

Daily energy consumption totals per building and meter type.
Use for day-level comparisons, weekday vs weekend patterns, and seasonal breakdowns.

Each row is one building × one meter type × one calendar day.

Key columns:
- `total_kwh` — total energy consumed that day
- `avg_hourly_kwh` — average load across all metered hours
- `peak_hourly_kwh` — maximum single-hour load that day
- `hours_with_data` — number of valid hourly readings (max 24)
- `daily_kwh_per_sqft` — daily EUI proxy (multiply by 365 to annualize)
- `season`, `weekday_type` — time slicing attributes

For site-level or monthly rollups use `metric_site_monthly`.
