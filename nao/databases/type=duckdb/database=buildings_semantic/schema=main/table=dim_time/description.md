# dim_time

**Dataset:** `main`

## Description

One row per unique hourly timestamp in the dataset (2016–2017). Adds business-friendly
time attributes for slicing by season, weekday type, and business hours.

Key columns:
- `ts` — the timestamp (matches `stg_meter_readings.ts` and all metric views)
- `season` — Winter, Spring, Summer, Autumn (Northern Hemisphere meteorological seasons)
- `weekday_type` — Weekday or Weekend
- `is_business_hours` — TRUE for 08:00–17:59 on weekdays (Mon–Fri)
