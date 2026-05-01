"""
Generate synthetic work orders and load all context data into buildings_semantic.duckdb.

Creates:
  - work_orders table  (synthetic facility maintenance tickets)
  - ref_eems table     (ASHRAE RP-1836 energy efficiency measures)

Usage:
    python src/generate_context_data.py
"""

import os
import csv
import random
import duckdb
from datetime import date, timedelta

DATA_DIR   = os.path.join(os.path.dirname(__file__), "..", "data")
DB_PATH    = os.path.join(DATA_DIR, "buildings_semantic.duckdb")
CONTEXT_DIR = os.path.join(DATA_DIR, "context")
EEMS_PATH  = os.path.join(CONTEXT_DIR, "ashrae_eems.csv")
WO_PATH    = os.path.join(CONTEXT_DIR, "work_orders.csv")

# A representative cross-section of BDG2 building IDs
BUILDINGS = [
    ("Bull_education_Roland",       "Bull",      "Education",                       216050),
    ("Cockatoo_education_Janet",    "Cockatoo",  "Education",                       322341),
    ("Hog_office_Darlene",          "Hog",       "Office",                          121714),
    ("Bear_education_Lashanda",     "Bear",      "Education",                       117814),
    ("Panther_lodging_Else",        "Panther",   "Lodging/residential",              37241),
    ("Robin_education_Mercedes",    "Robin",     "Education",                        56995),
    ("Panther_education_Emily",     "Panther",   "Education",                        68094),
    ("Shrew_office_Kenneth",        "Shrew",     "Office",                           18503),
    ("Cockatoo_education_Julio",    "Cockatoo",  "Education",                        98125),
    ("Wolf_education_Cody",         "Wolf",      "Education",                        28406),
    ("Bear_public_Orville",         "Bear",      "Public services",                  29063),
    ("Rat_assembly_Kaitlyn",        "Rat",       "Entertainment/public assembly",    26187),
    ("Gator_assembly_Lelia",        "Gator",     "Entertainment/public assembly",     3200),
    ("Lamb_education_Daniel",       "Lamb",      "Education",                        15414),
    ("Cockatoo_lodging_Fritz",      "Cockatoo",  "Lodging/residential",              19984),
]

WORK_ORDERS = [
    # (category, priority, description, resolution_notes)
    ("HVAC", "High",
     "Chiller plant running continuously overnight — energy spike detected on dashboard. Overnight setback schedule not active.",
     "Reprogrammed BAS overnight setback. Chiller now cycling off at 22:00. Estimated 18% overnight reduction."),
    ("HVAC", "Critical",
     "AHU-3 economizer damper stuck closed. Building pressurization off, high reheat energy all day.",
     "Replaced actuator on economizer damper. Economizer now operating in free-cooling mode during shoulder seasons."),
    ("HVAC", "Medium",
     "Steam trap failure in basement plant room. Visible condensate leak and steam loss since last week.",
     "Replaced 4 failed steam traps. Steam consumption expected to drop ~12% based on metered baseline."),
    ("HVAC", "Low",
     "Supply air temperature reset not functioning — SAT locked at 55°F regardless of load.",
     "Fixed BAS sensor calibration. SAT reset now responding to zone demand."),
    ("Electrical", "High",
     "Main electrical panel running near capacity during peak hours. Demand charges spiking on utility bills.",
     "Installed demand controller. Shifted non-critical loads to off-peak. Demand charge reduced by 22%."),
    ("Electrical", "Medium",
     "Parking structure lighting on 24/7 — no occupancy sensors installed. Confirmed waste during nights and weekends.",
     "Installed occupancy sensors across all 4 parking levels. Lighting now off when unoccupied."),
    ("Electrical", "Low",
     "Server room UPS units running at 40% load — very inefficient. Recommend consolidation.",
     "Consolidated to 2 UPS units from 5. Improved load factor to 85%. Estimated 8 MWh/year savings."),
    ("Envelope", "Medium",
     "Significant air infiltration around loading dock doors — cold air ingress causing heating overrun in winter.",
     "Installed high-speed roll-up doors with air curtains. Heating demand reduced noticeably in perimeter zones."),
    ("Envelope", "Low",
     "Roof membrane showing ponding water in 3 areas. Risk of insulation degradation and thermal bridging.",
     "Patched membrane and added tapered insulation in ponding areas. Roof U-value improved."),
    ("Lighting", "Low",
     "T12 fluorescent fixtures still in place in basement corridors. Very high wattage vs modern equivalents.",
     "Replaced 60 T12 fixtures with LED. 74% reduction in corridor lighting energy."),
    ("Lighting", "Medium",
     "Daylight sensors in open office not calibrated — electric lights at full output even on bright days.",
     "Recalibrated 40 daylight sensors. Lighting dimming correctly to 20-40% during peak daylight hours."),
    ("Plumbing", "High",
     "Domestic hot water circulation pump running continuously. No timer or demand control installed.",
     "Installed timer + demand control on DHW pump. Pump now off overnight. Hot water energy down ~30%."),
    ("HVAC", "Medium",
     "VAV boxes in east wing not communicating with BAS — running at full airflow constantly.",
     "Replaced 12 faulty VAV controllers. Airflow now demand-controlled. Reheat energy significantly reduced."),
    ("Envelope", "High",
     "Window seal failures on floors 3-5 south facade. Condensation visible, thermal camera shows major heat loss.",
     "Replaced seals on 28 windows. Heating load on south facade reduced. Payback estimated at 2.5 years."),
    ("HVAC", "Critical",
     "Cooling tower basin heaters running in summer — thermostat set incorrectly after winter. Enormous waste.",
     "Corrected thermostat setpoints. Basin heaters now off above 40°F ambient. Immediate electricity reduction."),
    ("Electrical", "Low",
     "Vending machines in break rooms running at full refrigeration 24/7 with no occupancy control.",
     "Installed VendingMiser controllers on 8 vending machines. Estimated 35% reduction in vending energy."),
    ("HVAC", "Medium",
     "Heat recovery unit bypass damper open — system not recovering exhaust heat during winter.",
     "Closed bypass damper and reprogrammed sequence. ERU now recovering ~65% of exhaust energy as designed."),
    ("Lighting", "High",
     "Exterior signage and façade lighting not on timer — confirmed running past midnight on weeknights.",
     "Programmed exterior lighting timer. All signage off by 23:00 on weekdays, 00:00 on weekends."),
    ("HVAC", "Low",
     "Boiler plant running lead/lag rotation not set up — same boiler running 100% of hours since commissioning.",
     "Configured lead/lag rotation schedule. Both boilers now sharing load equally, extending equipment life."),
    ("Electrical", "Medium",
     "Power factor below 0.85 on main transformer — utility penalty charges appearing on bills.",
     "Installed 200 kVAR capacitor bank. Power factor corrected to 0.97. Penalty charges eliminated."),
]

random.seed(42)


def generate_work_orders() -> list[dict]:
    records = []
    start_date = date(2016, 1, 1)
    end_date   = date(2017, 12, 31)
    wo_id = 1000

    for building, site, usage, sqft in BUILDINGS:
        n_tickets = random.randint(3, 8)
        for _ in range(n_tickets):
            cat, priority, desc, resolution = random.choice(WORK_ORDERS)
            days_offset = random.randint(0, (end_date - start_date).days)
            created = start_date + timedelta(days=days_offset)
            closed_offset = random.randint(1, 45)
            closed = created + timedelta(days=closed_offset)
            status = random.choices(["Closed", "Closed", "Closed", "In Progress", "Open"],
                                    weights=[60, 60, 60, 15, 5])[0]
            records.append({
                "work_order_id": f"WO-{wo_id}",
                "building_id":   building,
                "site_id":       site,
                "primaryspaceusage": usage,
                "sqft":          sqft,
                "created_date":  created.isoformat(),
                "closed_date":   closed.isoformat() if status == "Closed" else "",
                "priority":      priority,
                "category":      cat,
                "status":        status,
                "description":   desc,
                "resolution_notes": resolution if status == "Closed" else "",
            })
            wo_id += 1

    return records


def write_work_orders_csv(records: list[dict]) -> None:
    os.makedirs(CONTEXT_DIR, exist_ok=True)
    fieldnames = list(records[0].keys())
    with open(WO_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"  ✓ work_orders.csv       →  {len(records)} tickets written")


def load_into_duckdb() -> None:
    if not os.path.exists(DB_PATH):
        print(f"  ⚠  {os.path.basename(DB_PATH)} not found — run setup_buildings_layer.py first")
        return

    con = duckdb.connect(DB_PATH)

    # Work orders
    con.execute("DROP TABLE IF EXISTS work_orders")
    con.execute(f"""
        CREATE TABLE work_orders AS
        SELECT
            work_order_id,
            building_id,
            site_id,
            primaryspaceusage,
            sqft::DOUBLE AS sqft,
            created_date::DATE AS created_date,
            TRY_CAST(NULLIF(closed_date, '') AS DATE) AS closed_date,
            priority,
            category,
            status,
            description,
            NULLIF(resolution_notes, '') AS resolution_notes,
            DATEDIFF('day', created_date::DATE,
                COALESCE(NULLIF(closed_date, '')::DATE, CURRENT_DATE)) AS days_open
        FROM read_csv_auto('{WO_PATH}', all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM work_orders").fetchone()[0]
    print(f"  ✓ work_orders           →  {n} rows loaded into DuckDB")

    # ASHRAE EEMs reference table
    if os.path.exists(EEMS_PATH):
        con.execute("DROP TABLE IF EXISTS ref_eems")
        con.execute(f"""
            CREATE TABLE ref_eems AS
            SELECT
                eem_id::INTEGER AS eem_id,
                document,
                cat_lev1 AS category,
                cat_lev2 AS subcategory,
                eem_name AS measure_name
            FROM read_csv_auto('{EEMS_PATH}')
        """)
        n = con.execute("SELECT COUNT(*) FROM ref_eems").fetchone()[0]
        print(f"  ✓ ref_eems              →  {n} measures loaded into DuckDB")
    else:
        print("  ⚠  ashrae_eems.csv not found — skipping ref_eems")

    con.close()


if __name__ == "__main__":
    print("\n── Building context layer ──\n")
    records = generate_work_orders()
    write_work_orders_csv(records)
    load_into_duckdb()
    print("\nDone.\n")
