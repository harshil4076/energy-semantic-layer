---
name: monthly-cost-breakdown
description: Show monthly energy cost and consumption broken down by zone (kitchen, laundry, climate, unmetered). Triggers when user asks about monthly bills, monthly cost, monthly spending, or monthly consumption by zone.
---

# Monthly Cost Breakdown

## SQL

```sql
SELECT
    year,
    month_name,
    ROUND(kitchen_kwh, 2)            AS kitchen_kwh,
    ROUND(laundry_kwh, 2)            AS laundry_kwh,
    ROUND(climate_kwh, 2)            AS climate_kwh,
    ROUND(unmetered_kwh, 2)          AS unmetered_kwh,
    ROUND(total_consumption_kwh, 2)  AS total_kwh,
    ROUND(estimated_cost_eur, 2)     AS cost_eur
FROM metric_monthly
ORDER BY month_start;
```

## Output format

Present as a table. Suggest a stacked bar chart (zones as series, months on x-axis) and a line chart for total cost over time.
Note that costs are estimates using a flat €0.18/kWh tariff.
