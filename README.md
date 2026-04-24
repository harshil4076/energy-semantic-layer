# Energy Submetering — Semantic Layer Starter

Learn what a **semantic layer** is by building one from scratch on real energy data.

## The Idea

Raw data has cryptic column names (`Sub_metering_1`, `Global_active_power`) and
formulas buried in documentation. A semantic layer sits on top and says:

> "Kitchen Consumption" = `SUM(Sub_metering_1)`.
> "Estimated Cost" = `total_kwh × €0.18`.
> "Peak Hours" = 06:00–21:59.

Change a definition in one place → every query, dashboard, and report updates automatically.

## Dataset

**UCI Individual Household Electric Power Consumption** (Kaggle)
- ~2 million minute-level readings from a household near Paris
- December 2006 – November 2010 (47 months)
- 3 submetering channels + overall power, voltage, current

Download: https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set

## Quick Start

```bash
# Clone and enter
git clone <this-repo>
cd energy-semantic-layer

# Install dependencies
pip install -r requirements.txt

# Download data (option A: Kaggle CLI)
bash scripts/download_data.sh

# Download data (option B: manual)
# Download from Kaggle, unzip, place household_power_consumption.txt in data/

# Build the semantic layer
python src/setup_semantic_layer.py

# Run example queries
python src/run_sample_queries.py
```

## Semantic Layer Architecture

```
Raw CSV (household_power_consumption.txt)
  │
  ▼
┌─────────────────────────────────────────────┐
│  stg_readings (staging table)               │
│  Parse types, handle nulls, rename columns, │
│  derive unmetered consumption               │
└─────────┬───────────────────────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌──────────────┐
│dim_time│  │metric_hourly │──► metric_daily ──► metric_monthly
└────────┘  └──────────────┘
```

| Layer          | Purpose                                              |
|----------------|------------------------------------------------------|
| `stg_readings` | Clean raw data — one row per minute                  |
| `dim_time`     | Time attributes: season, peak/off-peak, weekday type |
| `metric_hourly`| Hourly aggregates per consumption zone               |
| `metric_daily` | Daily roll-ups with peak/off-peak split and cost     |
| `metric_monthly`| Monthly roll-ups for trend analysis                 |

## Key Metric Definitions

| Business Term            | Formula                                                |
|--------------------------|--------------------------------------------------------|
| Kitchen Consumption (Wh) | `SUM(sub_metering_1_wh)`                              |
| Laundry Consumption (Wh) | `SUM(sub_metering_2_wh)`                              |
| Climate Consumption (Wh) | `SUM(sub_metering_3_wh)` — water heater + AC          |
| Unmetered Consumption    | `global_active_power*1000/60 - sub1 - sub2 - sub3`    |
| Total Consumption        | Kitchen + Laundry + Climate + Unmetered                |
| Estimated Cost (€)       | `total_kwh × 0.18`                                    |
| Peak Hours               | 06:00 – 21:59                                          |

All definitions live in `src/setup_semantic_layer.py`. The YAML contract is in
`src/semantic_definitions.yml`.

## Exercises to Try

1. **Change the tariff** to €0.25 and re-run — watch costs update everywhere.
2. **Add a weekly metric view** (`metric_weekly`) following the daily pattern.
3. **Add a "high usage alert" flag** to `metric_daily` (e.g. days > 30 kWh).
4. **Connect Metabase or Superset** to the DuckDB file and build a dashboard.
5. **Port to Cube.dev** — rewrite the views as Cube schema files and serve via API.

## License

Dataset: CC BY 4.0 (Hébrail & Bérard, UCI ML Repository)
Code: MIT
