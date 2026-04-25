---
name: buildings-site-monthly
description: |
  Monthly consumption trend by site.
  Triggers: "monthly electricity by site", "how has consumption changed over time",
  "year over year by site", "seasonal trends for buildings", "campus monthly usage"
---

## Query

```sql
SELECT
    year,
    month,
    site_id,
    meter_type,
    ROUND(total_kwh, 0)                AS total_kwh,
    building_count,
    ROUND(monthly_kwh_per_sqft, 4)     AS monthly_kwh_per_sqft
FROM metric_site_monthly
WHERE meter_type = 'electricity'
ORDER BY site_id, year, month
```

## Output format

Return as a table with year, month, site_id, total_kwh, building_count.
Suggest a line chart with month on the x-axis, one line per site_id.
Note that data covers 2016–2017 only.
