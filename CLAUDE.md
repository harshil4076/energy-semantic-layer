# CLAUDE.md — Project Context for Claude Code

## What this project is

A **semantic layer** built on top of two energy datasets using DuckDB and SQL views. The semantic layer translates raw sensor columns into business-friendly metrics so analysts can query with plain terms instead of remembering schema details.

Two datasets are included:
1. **UCI Household** — minute-level residential power consumption (Paris, 2006–2010)
2. **Building Data Genome Project 2 (BDG2)** — hourly commercial/institutional building meter data (1,636 buildings, 8 meter types, 2016–2017)

## Project structure

```
energy-semantic-layer/
├── CLAUDE.md                  ← you are here
├── README.md                  ← user-facing docs
├── requirements.txt           ← Python dependencies (duckdb, pytest)
├── .gitignore
├── data/                      ← raw data goes here (gitignored)
│   ├── .gitkeep
│   └── buildings/             ← BDG2 CSVs go here (gitignored)
├── src/
│   ├── setup_semantic_layer.py         ← builds household DuckDB + views
│   ├── setup_buildings_layer.py        ← builds buildings DuckDB + views
│   ├── run_sample_queries.py           ← demo queries (household)
│   ├── semantic_definitions.yml        ← household metric contract
│   └── semantic_definitions_buildings.yml  ← buildings metric contract
├── tests/
│   └── test_semantic_layer.py          ← 25 pytest tests (household, no data needed)
├── nao/
│   ├── nao_config.yaml.template        ← committed config (no secrets)
│   ├── start.sh                        ← starts the chat server
│   ├── RULES.md                        ← agent rules for both datasets
│   ├── agent/skills/                   ← pre-built SQL skills
│   └── databases/                      ← nao context files (nao sync output)
└── scripts/
    └── download_data.sh                ← downloads household dataset via Kaggle CLI
```

## How to set up and run

```bash
# 1. Install deps
pip install -r requirements.txt

# 2a. Household dataset
# Download from https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set
# Place household_power_consumption.txt in data/
python src/setup_semantic_layer.py      # → data/energy_semantic.duckdb

# 2b. Buildings dataset
# Download from https://www.kaggle.com/datasets/claytonmiller/buildingdatagenomeproject2
# Place all CSVs in data/buildings/
python src/setup_buildings_layer.py     # → data/buildings_semantic.duckdb

# 3. Run sample queries (household)
python src/run_sample_queries.py

# 4. Run tests
python -m pytest tests/ -v

# 5. Start the nao chat agent (both databases)
bash nao/start.sh                       # → http://localhost:5005
```

## Key conventions

- Business metric definitions live in the respective `setup_*.py` file. Change them there and every downstream query picks up the new logic.
- The YAML files `src/semantic_definitions*.yml` are the human-readable contracts — keep them in sync when adding metrics.
- DuckDB files land at `data/energy_semantic.duckdb` and `data/buildings_semantic.duckdb` (both gitignored).
- Raw data is never committed — only the code that transforms it.
- nao API key lives in `nao/.env` (gitignored). Template at `nao/.env.example`.

## Common tasks

### Household layer
- **Add a new metric**: Add SQL to a view in `setup_semantic_layer.py`, document in `semantic_definitions.yml`, re-run setup.
- **Change a business rule** (tariff, peak hours): Edit constants at the top of `setup_semantic_layer.py` and re-run.

### Buildings layer
- **Add a new metric**: Add SQL to a view in `setup_buildings_layer.py`, document in `semantic_definitions_buildings.yml`, re-run setup.
- **Change business hours**: Edit `BUSINESS_HOURS_START` / `BUSINESS_HOURS_END` at the top of `setup_buildings_layer.py`.
- **Add a new meter type**: Add to the `METER_TYPES` list in `setup_buildings_layer.py`.

### Both datasets
- **Add a new time grain**: Follow the `metric_daily` → `metric_weekly` pattern in the relevant setup file.
- **Connect a BI tool**: Point it at either `.duckdb` file — views are queryable by any tool that speaks DuckDB or SQL.
- **Update nao context**: After changing views, run `nao sync` from the `nao/` directory to regenerate column/preview files.

## Buildings layer schema summary

| Table | Grain | Key columns |
|---|---|---|
| `stg_meter_readings` | building × meter_type × hour | ts, building_id, meter_type, kwh |
| `dim_building` | building_id | site_id, primaryspaceusage, sqft, sqm, timezone |
| `dim_time` | hour | season, weekday_type, is_business_hours |
| `metric_building_hourly` | building × meter_type × hour | kwh, kwh_per_sqft (EUI proxy) |
| `metric_building_daily` | building × meter_type × day | total_kwh, peak_hourly_kwh, daily_kwh_per_sqft |
| `metric_site_monthly` | site × meter_type × month | total_kwh, building_count, monthly_kwh_per_sqft |

EUI (Energy Use Intensity) = kWh/sqft. Multiply hourly `kwh_per_sqft` × 8760 or daily `daily_kwh_per_sqft` × 365 to annualize.

## Tech stack

- Python 3.9+
- DuckDB (in-process analytical database, zero external dependencies)
- nao analytics agent (chat UI on top of DuckDB)
- No ORM, no framework — just SQL views as the semantic layer
