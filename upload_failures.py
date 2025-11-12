import pandas as pd
from neo4j import GraphDatabase
import re

# -------------------------
# CONFIG
# -------------------------
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Pankaj1234"

EXCEL_PATH = "failures.xlsx"   # change to your Excel filename

# -------------------------
# UTILS
# -------------------------
def sanitize_label(name):
    name = str(name).strip()
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not name or not re.match(r"^[A-Za-z]", name):
        name = "X_" + name
    return name.upper()

# -------------------------
# CONNECT
# -------------------------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# -------------------------
# UPLOAD FUNCTION
# -------------------------
def upload_failures():
    df = pd.read_excel(EXCEL_PATH)
    df.fillna("", inplace=True)

    with driver.session() as session:
        for _, row in df.iterrows():
            zone = row["Zone"]
            division = row["Division"]
            section = row["Section"]
            gear_name = row["Mapped S&T Gear"]
            block_section = row["Block Section"]
            icms_id = str(row["ICMS Id"])
            smms_remark = str(row["SMMS Remark"])
            icms_remark = str(row["ICMS Remark"])
            start_time = str(row["start time"])
            end_time = str(row["End time"])
            start_date = str(row["Failure Start Date"])
            end_date = str(row["Failure End Date"])

            failure_props = {
                "ICMS_Id": icms_id,
                "SMMS_Remark": smms_remark,
                "ICMS_Remark": icms_remark,
                "Block_Section": block_section,
                "Start_Time": start_time,
                "End_Time": end_time,
                "Failure_Start_Date": start_date,
                "Failure_End_Date": end_date,
            }

            # merge failure node
            session.run("""
                MERGE (f:Failure {ICMS_Id: $ICMS_Id})
                SET f += $failure_props
            """, ICMS_Id=icms_id, failure_props=failure_props)

            # link to gear if exists
            session.run("""
                MATCH (g:Gear)
                WHERE toLower(g.name) = toLower($gear_name)
                MATCH (f:Failure {ICMS_Id: $ICMS_Id})
                MERGE (g)-[:HAS_FAILURE]->(f)
            """, gear_name=gear_name, ICMS_Id=icms_id)

            # link to zone/division/section if exists
            session.run("""
                OPTIONAL MATCH (z:Zone {name:$zone})
                OPTIONAL MATCH (d:Division {name:$division})
                OPTIONAL MATCH (s:Section {name:$section})
                MATCH (f:Failure {ICMS_Id:$ICMS_Id})
                FOREACH (_ IN CASE WHEN z IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (z)-[:HAS_FAILURE]->(f))
                FOREACH (_ IN CASE WHEN d IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (d)-[:HAS_FAILURE]->(f))
                FOREACH (_ IN CASE WHEN s IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (s)-[:HAS_FAILURE]->(f))
            """, zone=zone, division=division, section=section, ICMS_Id=icms_id)

    print("✅ All failures uploaded and linked successfully.")

# -------------------------
if __name__ == "__main__":
    upload_failures()
