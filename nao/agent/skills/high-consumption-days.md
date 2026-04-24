---
name: high-consumption-days
description: Find the days with the highest energy consumption or cost. Triggers when user asks about the most expensive days, highest usage days, top consumption days, or energy spikes.
---

# High Consumption Days

## SQL

```sql
SELECT
    date_day,
    ROUND(total_consumption_kwh, 2)  AS total_kwh,
    ROUND(estimated_cost_eur, 2)     AS cost_eur,
    ROUND(peak_kwh, 2)              AS peak_kwh,
    ROUND(offpeak_kwh, 2)           AS offpeak_kwh,
    readings_count
FROM metric_daily
ORDER BY total_consumption_kwh DESC
LIMIT 10;
```

## Output format

Present as a ranked table. Note any days where `readings_count` is significantly below 1440 (= 24 × 60) as they may have incomplete data.
