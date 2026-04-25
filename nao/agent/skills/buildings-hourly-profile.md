---
name: buildings-hourly-profile
description: |
  Average hourly load profile for a building type or site.
  Triggers: "hourly load profile for buildings", "when do offices use most electricity",
  "average hourly consumption pattern", "business hours vs after hours"
---

## Query

```sql
SELECT
    t.hour,
    b.primaryspaceusage,
    AVG(h.kwh)  AS avg_kwh,
    COUNT(*)    AS sample_count
FROM metric_building_hourly h
JOIN dim_time     t ON h.ts = t.ts
JOIN dim_building b ON h.building_id = b.building_id
WHERE h.meter_type = 'electricity'
GROUP BY t.hour, b.primaryspaceusage
ORDER BY b.primaryspaceusage, t.hour
```

## Output format

Return as a table with hour (0–23), primaryspaceusage, avg_kwh.
Suggest a line chart with hour on the x-axis, one line per space usage type.
Highlight the business hours window (08:00–17:59) in your commentary.
