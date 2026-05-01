# Data Sources Guide

## Energy meter data (metric_building_hourly / daily / monthly)

- Source: Building Data Genome Project 2 (BDG2)
- Coverage: 1,636 buildings, 8 meter types, hourly, 2016–2017
- Meter types: electricity, gas, hotwater, chilledwater, steam, water, irrigation, solar
- Units: kWh per hour
- EUI = kWh / sqft. Annualize: hourly × 8,760 or daily × 365
- Use `metric_building_daily` for day-level questions, `metric_site_monthly` for portfolio rollups

## Maintenance work orders (work_orders table)

- Source: Internal facility management system (CMMS export)
- Coverage: Subset of portfolio buildings, 2016–2017
- Key fields: building_id, category (HVAC/Electrical/Envelope/Lighting/Plumbing), priority, description, resolution_notes
- Text fields (description, resolution_notes) contain the operational context explaining energy anomalies
- Use to correlate energy spikes or drops with maintenance events

## Energy efficiency measures reference (ref_eems table)

- Source: ASHRAE RP-1836 (public dataset, Zenodo)
- Coverage: 3,490 named energy efficiency measures across all building system types
- Use to identify what interventions apply to a building's high-consumption systems
- Key fields: category (maps to HVAC/Lighting/Envelope etc), measure_name (plain English description)

## Building metadata (dim_building table)

- Source: BDG2 metadata.csv
- Key fields: building_id, site_id, primaryspaceusage, sqft, sqm, timezone, yearbuilt
- Join to any meter table on building_id to get building characteristics

## Sustainability goals and building notes

- See sustainability-goals.md for portfolio targets and site-level EUI benchmarks
- See building-notes.md for known issues, equipment history, and operational context per site
