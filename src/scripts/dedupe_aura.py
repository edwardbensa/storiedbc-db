from src.utils.ops_aura import deduplicate_nodes
from src.utils.connectors import connect_auradb
from src.utils.transform_for_aura import collection_label_map

neo4j_driver = connect_auradb()

def dedupe_all(driver, identifier="name"):
    labels = list(collection_label_map.values())
    for label in labels:
        deduplicate_nodes(driver, label, identifier=identifier)

# deduplicate_nodes(neo4j_driver, "BookVersion", "asin")
dedupe_all(neo4j_driver, "name")
