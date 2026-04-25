---
name: buildings-eui-ranking
description: |
  Rank buildings or sites by Energy Use Intensity (EUI).
  Triggers: "which buildings use the most energy per sqft", "EUI ranking", "most efficient buildings",
  "energy intensity by site", "benchmark buildings"
---

## Query

```sql
SELECT
    b.building_id,
    b.site_id,
    b.primaryspaceusage,
    b.sqft,
    SUM(d.total_kwh)                          AS total_kwh_2016_2017,
    SUM(d.total_kwh) / NULLIF(b.sqft, 0)      AS total_kwh_per_sqft,
    -- Annualized EUI (divide by 2 years of data)
    SUM(d.total_kwh) / NULLIF(b.sqft, 0) / 2  AS annual_kwh_per_sqft
FROM metric_building_daily d
JOIN dim_building b ON d.building_id = b.building_id
WHERE d.meter_type = 'electricity'
  AND b.sqft > 0
GROUP BY b.building_id, b.site_id, b.primaryspaceusage, b.sqft
ORDER BY annual_kwh_per_sqft DESC
LIMIT 20
```

## Output format

Return as a table with columns: building_id, site_id, primaryspaceusage, sqft,
annual_kwh_per_sqft (rounded to 2 decimal places).

Note that EUI varies significantly by building type — hospitals and labs typically
show higher EUI than offices. Always note the space usage when comparing.
Suggest a horizontal bar chart to visualize the ranking.
