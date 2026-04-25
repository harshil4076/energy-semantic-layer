# dim_building

**Dataset:** `main`

## Description

One row per building from the Building Data Genome Project 2 (BDG2).
Contains building characteristics: size, type, site/campus, and which meter types are available.

Key columns for analysis:
- `building_id` — unique identifier (format: SiteName_usagetype_PersonName, e.g. `Panther_lodging_Dean`)
- `site_id` — campus or portfolio the building belongs to (e.g. `Panther`, `Fox`, `Bear`)
- `primaryspaceusage` — space type: Office, Lodging/residential, Education, Retail, etc.
- `sqft` — floor area in square feet; used to calculate EUI (Energy Use Intensity)
- `sqm` — floor area in square meters
- `timezone` — IANA timezone, e.g. `US/Eastern`

Join this table to any metric view on `building_id` to enrich with building attributes.
