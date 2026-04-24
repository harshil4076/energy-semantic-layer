---
name: seasonal-comparison
description: Compare average daily energy consumption and cost across seasons. Triggers when user asks about seasonal differences, summer vs winter usage, which season uses the most energy, or seasonal patterns.
---

# Seasonal Comparison

## SQL

```sql
SELECT
    t.season,
    ROUND(AVG(d.total_consumption_kwh), 2)  AS avg_daily_kwh,
    ROUND(AVG(d.estimated_cost_eur), 2)     AS avg_daily_cost_eur,
    ROUND(AVG(d.kitchen_kwh), 2)            AS avg_kitchen_kwh,
    ROUND(AVG(d.laundry_kwh), 2)            AS avg_laundry_kwh,
    ROUND(AVG(d.climate_kwh), 2)            AS avg_climate_kwh,
    ROUND(AVG(d.peak_kwh), 2)              AS avg_peak_kwh,
    ROUND(AVG(d.offpeak_kwh), 2)           AS avg_offpeak_kwh,
    COUNT(*) AS days_sampled
FROM metric_daily d
JOIN dim_time t ON d.date_day = t.date_day
GROUP BY t.season
ORDER BY avg_daily_kwh DESC;
```

## Output format

Present as a table ordered by average daily consumption. Suggest a grouped bar chart comparing zones across seasons.
