# metric_building_hourly

**Dataset:** `main`

## Description

Hourly energy consumption per building and meter type, enriched with building size
and time attributes. Use for load profile analysis, anomaly detection, and
business-hours vs after-hours comparisons.

Each row is one building × one meter type × one hour.

Key columns:
- `kwh` — energy consumed in that hour (kWh)
- `meter_type` — one of: electricity, gas, hotwater, chilledwater, steam, water, irrigation, solar
- `kwh_per_sqft` — EUI proxy: kWh divided by building floor area in sqft. Multiply by 8760 to get annual kWh/sqft
- `is_business_hours` — TRUE for 08:00–17:59 weekdays
- `season`, `weekday_type` — time slicing attributes

This is the most granular metric view. For summary questions prefer `metric_building_daily` or `metric_site_monthly`.
