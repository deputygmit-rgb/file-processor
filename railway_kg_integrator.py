from neo4j import GraphDatabase
import json
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Call2@007"))
def create_railway_hierarchy(tx, hierarchy):
    for zone_name, zone_data in hierarchy.items():
        tx.run("MERGE (z:Zone {name: $zone})", zone=zone_name)

        # --- Divisions ---
        for division in zone_data.get("Divisions", []):
            tx.run("""
                MERGE (z:Zone {name: $zone})
                MERGE (d:Division {name: $division})
                MERGE (z)-[:HAS_DIVISION]->(d)
            """, zone=zone_name, division=division)

        # --- Departments & Functions ---
        for dept_name, functions in zone_data.get("Departments", {}).items():
            tx.run("""
                MERGE (dep:Department {name: $dept})
                MERGE (z:Zone {name: $zone})
                MERGE (z)-[:HAS_DEPARTMENT]->(dep)
            """, zone=zone_name, dept=dept_name)

            for fn in functions:
                tx.run("""
                    MERGE (f:Function {name: $function})
                    MERGE (dep:Department {name: $dept})
                    MERGE (dep)-[:PERFORMS]->(f)
                """, dept=dept_name, function=fn)


def load_railway_hierarchy(json_path="railway_hierarchy.json"):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print("🚆 Building Indian Railways hierarchy in Neo4j...")
    with driver.session() as session:
        session.execute_write(create_railway_hierarchy, data["Indian Railways"]["Zones"])
    print("✅ Hierarchy successfully added to Neo4j!")
