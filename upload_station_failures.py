import pandas as pd
from neo4j import GraphDatabase
import re

# --------------------------
# CONFIG
# --------------------------
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Pankaj1234"
EXCEL_PATH = "station_failures.xlsx"  # path to your Excel file

# --------------------------
# CONNECT
# --------------------------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# --------------------------
# HELPERS
# --------------------------
def sanitize_text(t):
    return str(t).strip() if pd.notna(t) else ""

# --------------------------
# UPLOAD FUNCTION
# --------------------------
def upload_station_failures():
    df = pd.read_excel(EXCEL_PATH)
    df.fillna("", inplace=True)

    with driver.session() as session:
        for _, row in df.iterrows():
            # Extract relevant fields
            zone = sanitize_text(row.get("Zone"))
            division = sanitize_text(row.get("Division"))
            section = sanitize_text(row.get("Section"))
            station = sanitize_text(row.get("Station Code"))
            department = sanitize_text(row.get("Department"))
            gear_name = sanitize_text(row.get("Gear / Equipment Name"))
            failure_id = sanitize_text(row.get("Failure Entry No"))
            failure_on = sanitize_text(row.get("Failure On"))
            rectification_on = sanitize_text(row.get("Rectification On"))
            cause = sanitize_text(row.get("Cause of Failure"))
            remarks = sanitize_text(row.get("Remarks"))
            action = sanitize_text(row.get("Action Performed"))
            duration = sanitize_text(row.get("Failure Duration(In Minutes)"))

            # ✅ Create Failure node
            failure_props = {
                "Failure_Entry_No": failure_id,
                "Failure_On": failure_on,
                "Rectification_On": rectification_on,
                "Cause": cause,
                "Remarks": remarks,
                "Action": action,
                "Duration_Min": duration,
                "Department": department,
            }

            session.run("""
                MERGE (f:Failure {Failure_Entry_No: $Failure_Entry_No})
                SET f += $props
            """, Failure_Entry_No=failure_id, props=failure_props)

            # ✅ Link to Station
            if station:
                session.run("""
                    MERGE (s:Station {code:$station})
                    MERGE (s)-[:HAS_FAILURE]->(f:Failure {Failure_Entry_No:$Failure_Entry_No})
                """, station=station, Failure_Entry_No=failure_id)

            # ✅ Link to Gear
            if gear_name:
                session.run("""
                    MATCH (g:Gear)
                    WHERE toLower(g.name) = toLower($gear_name)
                    MATCH (f:Failure {Failure_Entry_No:$Failure_Entry_No})
                    MERGE (g)-[:HAS_FAILURE]->(f)
                """, gear_name=gear_name, Failure_Entry_No=failure_id)

            # ✅ Link to Zone/Division/Section hierarchy (if exists)
            session.run("""
                OPTIONAL MATCH (z:Zone {name:$zone})
                OPTIONAL MATCH (d:Division {name:$division})
                OPTIONAL MATCH (s:Section {name:$section})
                MATCH (f:Failure {Failure_Entry_No:$Failure_Entry_No})
                FOREACH (_ IN CASE WHEN z IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (z)-[:HAS_FAILURE]->(f))
                FOREACH (_ IN CASE WHEN d IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (d)-[:HAS_FAILURE]->(f))
                FOREACH (_ IN CASE WHEN s IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (s)-[:HAS_FAILURE]->(f))
            """, zone=zone, division=division, section=section, Failure_Entry_No=failure_id)

            # ✅ Link to Cause node (optional)
            if cause:
                session.run("""
                    MERGE (c:FailureCause {name:$cause})
                    MERGE (f:Failure {Failure_Entry_No:$Failure_Entry_No})
                    MERGE (f)-[:HAS_CAUSE]->(c)
                """, cause=cause, Failure_Entry_No=failure_id)

    print("✅ Station failure data uploaded successfully!")

# --------------------------
if __name__ == "__main__":
    upload_station_failures()
