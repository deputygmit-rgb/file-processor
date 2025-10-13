# kg_helpers.py
from datetime import datetime
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Call2@007"))
# ------------------------------
# MAIN HANDLER
# ------------------------------
def handle_kg_integration(cleaned_data, inserted_id, filename):
    """
    Handles all Knowledge Graph enrichment operations after file upload.
    """
    try:
        with driver.session() as session:
            # Assets
            if "assets" in cleaned_data:
                for asset in cleaned_data["assets"]:
                    add_entity_node(session, asset, label="Asset", file_id=str(inserted_id))

            # Projects
            if "projects" in cleaned_data:
                for project in cleaned_data["projects"]:
                    add_entity_node(session, project, label="Project", file_id=str(inserted_id))

            # Failures
            if "failures" in cleaned_data:
                for failure in cleaned_data["failures"]:
                    add_entity_node(session, failure, label="Failure", file_id=str(inserted_id))

            # Generic entities
            if "entities" in cleaned_data:
                for entity in cleaned_data["entities"]:
                    add_entity_node(session, entity, label="Entity", file_id=str(inserted_id))

            # Nodes
            if "nodes" in cleaned_data:
                for node in cleaned_data["nodes"]:
                    add_graph_node(session, node, file_id=str(inserted_id))

            print(f"✅ KG enrichment completed for {filename}")

    except Exception as e:
        print(f"⚠️ KG integration failed for {filename}: {e}")


# ------------------------------
# NEO4J ENTITY + NODE OPERATIONS
# ------------------------------
def add_entity_node(session, entity, label="Entity", file_id=None):
    """
    Adds an entity to Neo4j with optional properties.
    """
    try:
        name = entity.get("name") or f"Unnamed_{label}"
        props = {k: v for k, v in entity.items() if k != "name"}
        props["file_id"] = file_id
        props["created_at"] = datetime.utcnow().isoformat()

        query = f"""
        MERGE (n:{label} {{name: $name}})
        SET n += $props
        RETURN n.name AS name
        """
        session.run(query, name=name, props=props)
        print(f"🧩 Added {label}: {name}")
    except Exception as e:
        print(f"Error adding {label}: {e}")


def add_graph_node(session, node, file_id=None):
    """
    Adds a general node with relationships.
    """
    try:
        node_id = node.get("id") or f"Node_{file_id}"
        node_type = node.get("type", "Generic")
        relations = node.get("relations", [])

        query = f"""
        MERGE (n:{node_type} {{id: $node_id}})
        SET n.file_id = $file_id, n.created_at = datetime()
        RETURN n.id AS id
        """
        session.run(query, node_id=node_id, file_id=file_id)

        for rel in relations:
            target = rel.get("target")
            rel_type = rel.get("type", "LINKED_TO").upper()
            if target:
                rel_query = f"""
                MATCH (a:{node_type} {{id: $node_id}})
                MERGE (b {{name: $target}})
                MERGE (a)-[r:{rel_type}]->(b)
                RETURN type(r)
                """
                session.run(rel_query, node_id=node_id, target=target)
        print(f"🔗 Added graph node: {node_id}")

    except Exception as e:
        print(f"Error adding graph node: {e}")
