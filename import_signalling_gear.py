"""
import_signalling_gear.py
-------------------------------------
Builds a Neo4j Knowledge Graph from Signalling & Telecom gear Excel data.

Excel Format:
-------------------------------------
Sr. No | Zone | Division | Section | Location Type | Location | Asset Type | Gear Name/Number
-------------------------------------

Example Hierarchy Created:
(Zone)-[:HAS_DIVISION]->(Division)
       -[:HAS_SECTION]->(Section)
          -[:HAS_LOCATION_TYPE]->(LocationType)
             -[:HAS_LOCATION]->(Location)
                -[:HAS_ASSET_TYPE]->(AssetType)
                   -[:HAS_GEAR]->(Gear)
"""

import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

# ---------------- CONFIG ----------------
EXCEL_PATH = "signalling_telecom_gear.xlsx"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Pankaj1234"

# ----------------------------------------
def load_excel(path):
    df = pd.read_excel(path, dtype=str)
    df = df.fillna("")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "gear_name/number" in df.columns:
        df.rename(columns={"gear_name/number": "gear"}, inplace=True)
    elif "gear_name_number" in df.columns:
        df.rename(columns={"gear_name_number": "gear"}, inplace=True)
    return df

# ----------------------------------------
def create_gear_hierarchy(tx, row):
    """Create hierarchical nodes and relationships for each row."""
    zone = row.get("zone", "").strip()
    division = row.get("division", "").strip()
    section = row.get("section", "").strip()
    location_type = row.get("location_type", "").strip()
    location = row.get("location", "").strip()
    asset_type = row.get("asset_type", "").strip()
    gear = row.get("gear", "").strip()

    # Skip empty or incomplete rows
    if not zone or not division or not gear:
        return

    query = """
    MERGE (z:Zone {name: $zone})
      ON CREATE SET z.created_at = datetime()
    MERGE (d:Division {name: $division})
      ON CREATE SET d.created_at = datetime()
    MERGE (z)-[:HAS_DIVISION]->(d)

    MERGE (s:Section {name: $section})
      ON CREATE SET s.created_at = datetime()
    MERGE (d)-[:HAS_SECTION]->(s)

    MERGE (lt:LocationType {name: $location_type})
      ON CREATE SET lt.created_at = datetime()
    MERGE (s)-[:HAS_LOCATION_TYPE]->(lt)

    MERGE (l:Location {name: $location})
      ON CREATE SET l.created_at = datetime()
    MERGE (lt)-[:HAS_LOCATION]->(l)

    MERGE (a:AssetType {name: $asset_type})
      ON CREATE SET a.created_at = datetime()
    MERGE (l)-[:HAS_ASSET_TYPE]->(a)

    MERGE (g:Gear {name: $gear})
      ON CREATE SET g.created_at = datetime()
    MERGE (a)-[:HAS_GEAR]->(g)

    // Optionally link to department
    MERGE (dept:Department {name: 'Signal & Telecom'})
    MERGE (dept)-[:OWNS]->(a)
    """
    tx.run(
        query,
        zone=zone,
        division=division,
        section=section,
        location_type=location_type,
        location=location,
        asset_type=asset_type,
        gear=gear,
    )

# ----------------------------------------
def import_gear_data(df, uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Importing gears"):
            session.execute_write(create_gear_hierarchy, row)
    driver.close()
    print("✅ Completed graph import.")

# ----------------------------------------
if __name__ == "__main__":
    df = load_excel(EXCEL_PATH)
    print(f"Loaded {len(df)} rows from Excel.")
    import_gear_data(df, NEO4J_URI, NEO4J_USER, NEO4J_PASS)
    print("✅ All data imported successfully into Neo4j.")
