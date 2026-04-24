# Energy Submetering — Semantic Layer + AI Chat Agent

A **semantic layer** built on real household energy data, with an AI chat agent on top so anyone can query it in plain English and get visualizations.

![demo](demo.gif)

## What is a Semantic Layer?

Raw data has cryptic column names (`Sub_metering_1`, `Global_active_power`) and formulas buried in documentation. A semantic layer sits on top and translates them into business-friendly terms:

> "Kitchen Consumption" = `SUM(Sub_metering_1)`
> "Estimated Cost" = `total_kwh × €0.18`
> "Peak Hours" = 06:00–21:59

Change a definition in one place and every query, dashboard, and report updates automatically.

## Dataset

**UCI Individual Household Electric Power Consumption**
- ~2 million minute-level readings from a household near Paris
- December 2006 – November 2010 (47 months)
- 3 submetering channels + overall power, voltage, and current

### Download instructions

1. Go to https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set
2. Sign in to Kaggle and click **Download**
3. Unzip the downloaded archive
4. Place `household_power_consumption.txt` directly inside the `data/` folder:

```
energy-semantic-layer/
└── data/
    └── household_power_consumption.txt   ← file goes here
```

The `data/` folder is gitignored — the raw file is never committed.

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/harshil4076/energy-semantic-layer
cd energy-semantic-layer
pip install -r requirements.txt
```

### 2. Build the semantic layer

This loads the raw CSV into DuckDB and creates all the metric views:

```bash
python src/setup_semantic_layer.py
```

Expected output:
```
── Building semantic layer in energy_semantic.duckdb ──

  ✓ stg_readings (staging table)
    → 2,049,280 rows loaded

  ✓ dim_time (time dimension view)
  ✓ metric_hourly (hourly aggregates)
  ✓ metric_daily (daily aggregates)
  ✓ metric_monthly (monthly aggregates)
```

The database is saved at `data/energy_semantic.duckdb`.

### 3. Run example queries (optional)

```bash
python src/run_sample_queries.py
```

This runs 7 sample queries against the semantic layer — seasonal averages, top consumption days, hourly load profiles, year-over-year comparisons, and more.

### 4. Run the tests

```bash
python -m pytest tests/ -v
```

25 tests covering the staging layer, dimension logic, and all metric views. No data file required — tests use an in-memory DuckDB with synthetic data.

---

## AI Chat Agent (nao)

The `nao/` directory contains an [nao](https://github.com/getnao/nao) analytics agent configured to query the semantic layer. It provides a chat UI where you can ask questions in plain English and get back tables and charts.

### How it works

nao connects to `data/energy_semantic.duckdb` and uses the context files in `nao/databases/` and `nao/agent/skills/` to understand the data before generating SQL. The `RULES.md` file tells the agent which views to use for which question types, what the business definitions are (tariff, peak hours, zone names), and how to format answers.

When you ask "what was the most expensive month?", nao:
1. Reads the context to understand that `metric_monthly` has `estimated_cost_eur`
2. Generates the appropriate SQL
3. Runs it against the DuckDB file
4. Returns the result with a suggested visualization

### Add your Anthropic API key

Create a file at `nao/.env` (this file is gitignored and never committed):

```bash
cp nao/.env.example nao/.env   # if the example exists, or create manually
```

Edit `nao/.env` and add your key:

```
ANTHROPIC_API_KEY=sk-ant-...your-key-here...
```

Get a key from https://console.anthropic.com.

### Start the chat server

```bash
bash nao/start.sh
```

The start script:
1. Loads `nao/.env` into the environment
2. Generates `nao/nao_config.yaml` from `nao/nao_config.yaml.template` with the key substituted
3. Starts the nao server

Open **http://localhost:5005** in your browser.

### Example questions to ask

**Cost and consumption**
- "How much did electricity cost each month in 2007?"
- "Which consumption zone (kitchen, laundry, climate) costs the most over the year?"
- "What were the top 10 most expensive days on record?"

**Patterns and comparisons**
- "Show me the average hourly load profile — which hour uses the most energy?"
- "Do we use more energy on weekends or weekdays?"
- "How does winter consumption compare to summer?"

**Trends**
- "Show year-over-year total consumption and cost"
- "Which month had the highest climate (water heater/AC) usage?"

**Anomalies**
- "Were there any days with voltage outside the normal 220–240V range?"

Add "as a line chart" or "as a stacked bar chart" to any question to get a visualization.

---

## How the Semantic Layer is Built

The full build is in [`src/setup_semantic_layer.py`](src/setup_semantic_layer.py). Business constants are defined at the top of that file — changing them re-applies to every view on the next run.

```
Raw CSV  →  stg_readings  →  dim_time
                          →  metric_hourly  →  metric_daily  →  metric_monthly
```

### Layer 1 — Staging (`stg_readings`)

The raw CSV has semicolon-separated columns with names like `Sub_metering_1` and dates in `DD/MM/YYYY` format. The staging table:
- Parses date + time into a proper timestamp
- Renames columns to readable names (`sub_metering_1_wh` → "kitchen")
- Casts all sensor values to `DOUBLE`
- Derives unmetered consumption: `global_active_power × 1000/60 − sub1 − sub2 − sub3`, clamped to zero to handle sensor noise
- Drops the ~1.25% of rows where `Global_active_power` is null

### Layer 2 — Dimensions (`dim_time`)

One row per unique timestamp. Adds business attributes used for slicing:

| Attribute | Definition |
|---|---|
| `peak_period` | Peak = 06:00–21:59, Off-Peak = 22:00–05:59 |
| `season` | Winter (DJF), Spring (MAM), Summer (JJA), Autumn (SON) |
| `weekday_type` | Weekday or Weekend |

### Layer 3 — Metrics

Pre-aggregated at three grains, all built by rolling up from the previous level:

| View | Grain | Key metrics |
|---|---|---|
| `metric_hourly` | 1 hour | Wh per zone, avg voltage, max current, cost |
| `metric_daily` | 1 day | kWh per zone, peak/off-peak split, cost |
| `metric_monthly` | 1 month | kWh per zone, total cost, days with data |

Cost is calculated as `total_consumption_kwh × €0.18` (approximate French residential tariff, EDF Bleu). This is defined as `TARIFF_EUR_PER_KWH` at the top of `setup_semantic_layer.py`.

### How nao uses the semantic layer

The nao agent never queries `stg_readings` directly — it is excluded via `nao/.naoignore` and `nao/nao_config.yaml`. The context files in `nao/databases/` describe each view's columns and purpose in plain English, and `nao/RULES.md` enforces which view to use for which question type. Pre-built skills in `nao/agent/skills/` provide ready-made SQL for the most common queries (monthly cost breakdown, hourly load profile, seasonal comparison, high-consumption days).

The human-readable metric contract is also documented in [`src/semantic_definitions.yml`](src/semantic_definitions.yml).

---

## Project Structure

```
energy-semantic-layer/
├── data/                          ← gitignored; put raw .txt file here
├── src/
│   ├── setup_semantic_layer.py    ← builds DuckDB + all views (edit business rules here)
│   ├── run_sample_queries.py      ← demo queries
│   └── semantic_definitions.yml  ← human-readable metric contract
├── tests/
│   └── test_semantic_layer.py    ← 25 pytest tests, no data file needed
├── nao/
│   ├── nao_config.yaml.template  ← committed config template (no secrets)
│   ├── nao_config.yaml           ← gitignored; generated by start.sh
│   ├── .env                      ← gitignored; add your ANTHROPIC_API_KEY here
│   ├── start.sh                  ← start the chat server
│   ├── RULES.md                  ← agent business rules
│   ├── agent/skills/             ← pre-built SQL skills for common questions
│   └── databases/                ← table context files (auto-generated by nao sync)
├── scripts/
│   └── download_data.sh          ← optional: downloads via Kaggle CLI
└── requirements.txt
```

## Exercises to Try

1. **Change the tariff** — edit `TARIFF_EUR_PER_KWH` in `setup_semantic_layer.py`, re-run setup, ask nao "what's the monthly cost now?"
2. **Add a weekly metric view** — create `metric_weekly` following the daily pattern, run `nao sync` to pick it up
3. **Add a high-usage alert flag** to `metric_daily` (e.g. days > 30 kWh) and add it to `nao/RULES.md`
4. **Connect Metabase or Superset** directly to `data/energy_semantic.duckdb`

## License

Dataset: CC BY 4.0 (Hébrail & Bérard, UCI ML Repository)
Code: MIT
