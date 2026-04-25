---
name: buildings-meter-breakdown
description: |
  Break down energy consumption by meter type for a site or across all sites.
  Triggers: "which energy source uses most", "gas vs electricity", "meter type breakdown",
  "what's the split between heating and cooling", "energy mix by site"
---

## Query

```sql
SELECT
    site_id,
    meter_type,
    SUM(total_kwh)                               AS total_kwh,
    ROUND(100.0 * SUM(total_kwh) /
        SUM(SUM(total_kwh)) OVER (PARTITION BY site_id), 1) AS pct_of_site_total
FROM metric_building_daily
GROUP BY site_id, meter_type
ORDER BY site_id, total_kwh DESC
```

## Output format

Return as a table grouped by site_id with columns: meter_type, total_kwh, pct_of_site_total.
Suggest a stacked bar chart with sites on the x-axis and meter types as stacked segments.
