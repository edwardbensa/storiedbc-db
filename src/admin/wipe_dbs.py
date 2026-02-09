"""Wipe databases and start over"""

# Imports
from loguru import logger
from src.utils.connectors import connect_mongodb, connect_auradb
from src.utils.polyglot import clear_all_nodes
from src.utils.mongo_ops import drop_all_collections


# Connect to databases
main_db = connect_mongodb()
staging_db = connect_mongodb("staging")
neo4j_driver = connect_auradb()

def main(wipe: str="all"):
    """Choose which database to wipe"""
    if wipe == "mongo_main":
        logger.warning("Dropping all collections in main MongoDB...")
        drop_all_collections(main_db)
        logger.success("MongoDB collections successfully dropped...")
    elif wipe == "mongo_staging":
        logger.warning("Dropping all collections in staging MongoDB...")
        drop_all_collections(staging_db)
        logger.success("MongoDB collections successfully dropped...")
    elif wipe == "aura":
        logger.warning("Clearing all nodes and relationships in AuraDB...")
        clear_all_nodes(neo4j_driver)
        logger.success("AuraDB nodes and relationships successfully cleared...")
    else:
        logger.warning("Clearing all data from MongoDB and AuraDB...")
        drop_all_collections(main_db)
        drop_all_collections(staging_db)
        clear_all_nodes(neo4j_driver)
        logger.success("MongoDB and AuraDB nodes successfully wiped...")

# Run
if __name__ == "__main__":
    main("all")
