# metric_site_monthly

**Dataset:** `main`

## Description

Monthly energy rollup per site (campus/portfolio) and meter type.
Use for portfolio benchmarking, cross-site EUI comparisons, and year-over-year trends.

Each row is one site × one meter type × one calendar month.

Key columns:
- `total_kwh` — total site consumption for the month (all buildings combined)
- `avg_building_kwh` — average per-building consumption
- `building_count` — number of buildings contributing data
- `monthly_kwh_per_sqft` — site EUI proxy: total kWh / total sqft for the month

For individual building questions use `metric_building_daily`.
