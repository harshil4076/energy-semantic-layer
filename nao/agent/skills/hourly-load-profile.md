---
name: hourly-load-profile
description: Show the average power draw or consumption by hour of day. Triggers when user asks about hourly usage, daily load profile, which hour uses the most energy, peak hours, or when energy is most consumed.
---

# Hourly Load Profile

## SQL

```sql
SELECT
    hour_of_day,
    ROUND(AVG(avg_active_power_kw), 3)  AS avg_power_kw,
    ROUND(AVG(kitchen_wh), 1)           AS avg_kitchen_wh,
    ROUND(AVG(laundry_wh), 1)           AS avg_laundry_wh,
    ROUND(AVG(climate_wh), 1)           AS avg_climate_wh,
    ROUND(AVG(unmetered_wh), 1)         AS avg_unmetered_wh,
    ROUND(AVG(estimated_cost_eur), 4)   AS avg_cost_eur
FROM metric_hourly
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

## Output format

Present as a table and suggest a line chart (hour_of_day on x-axis, avg_power_kw on y-axis).
Highlight that hours 6–21 are peak-tariff hours.
