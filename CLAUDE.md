# CLAUDE.md — Project Context for Claude Code

## What this project is

A **semantic layer** built on top of the UCI Individual Household Electric Power Consumption dataset using DuckDB and SQL views. The semantic layer translates raw sensor columns into business-friendly metrics (kitchen consumption, estimated cost, peak vs off-peak) so analysts can query with plain terms instead of remembering schema details.

## Project structure

```
energy-semantic-layer/
├── CLAUDE.md                  ← you are here
├── README.md                  ← user-facing docs
├── requirements.txt           ← Python dependencies (just duckdb)
├── .gitignore
├── data/                      ← raw data goes here (gitignored)
│   └── .gitkeep
├── src/
│   ├── setup_semantic_layer.py    ← creates DuckDB + all views
│   ├── run_sample_queries.py      ← demo queries against the layer
│   └── semantic_definitions.yml   ← data dictionary / metric contract
└── scripts/
    └── download_data.sh           ← downloads dataset via Kaggle CLI
```

## How to set up and run

```bash
# 1. Install deps
pip install -r requirements.txt

# 2. Get the data (requires Kaggle CLI + API token)
bash scripts/download_data.sh
# OR manually download from:
# https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set
# and place household_power_consumption.txt in data/

# 3. Build the semantic layer
python src/setup_semantic_layer.py

# 4. Run sample queries
python src/run_sample_queries.py
```

## Key conventions

- All business metric definitions live in `src/setup_semantic_layer.py`. Change them there and every downstream query picks up the new logic.
- The YAML file `src/semantic_definitions.yml` is the human-readable contract — keep it in sync when adding metrics.
- DuckDB database file lands at `data/energy_semantic.duckdb` (gitignored).
- Raw data is never committed — only the code that transforms it.

## Common tasks

- **Add a new metric**: Add the SQL to a view in `setup_semantic_layer.py`, document it in `semantic_definitions.yml`, then re-run setup.
- **Change a business rule** (e.g. tariff, peak hours): Edit the constants at the top of `setup_semantic_layer.py` and re-run.
- **Add a new time grain**: Create a new `CREATE VIEW metric_weekly AS ...` following the pattern of `metric_daily`.
- **Connect a BI tool**: Point it at `data/energy_semantic.duckdb` — views are queryable by any tool that speaks DuckDB or SQL.

## Tech stack

- Python 3.9+
- DuckDB (in-process analytical database, zero external dependencies)
- No ORM, no framework — just SQL views as the semantic layer
