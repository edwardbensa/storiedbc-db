"""Create Neo4j indexes and vector search for GraphRAG."""

from loguru import logger
from src.utils.connectors import connect_auradb


def create_aura_indexes(driver):
    """Create property indexes for fast lookups."""
    indexes = {
        "Book": ["title", "genre", "publication_year"],
        "User": ["handle"],
        "Club": ["handle"],
        "Creator": ["name"],
        "Publisher": ["name"],
        "BookVersion": ["isbn_13", "asin"],
    }

    with driver.session() as session:
        for label, props in indexes.items():
            for prop in props:
                idx_name = f"{label.lower()}_{prop}_idx"
                query = f"""
                CREATE INDEX {idx_name} IF NOT EXISTS
                FOR (n:{label}) ON (n.{prop})
                """
                try:
                    session.run(query)
                    logger.info(f"Created index: {idx_name}")
                except Exception as e: # pylint: disable=W0718
                    logger.warning(f"Index {idx_name} failed: {e}")


def create_vector_index(driver):
    """Create vector index for description embeddings (GraphRAG)."""
    with driver.session() as session:
        result = session.run("CALL dbms.components() YIELD versions RETURN versions[0] as version")
        version = result.single()["version"]

        if not version.startswith("5."):
            logger.warning(f"Vector search requires Neo4j 5.x (current: {version})")
            return

        # Create vector index
        query = """
        CREATE VECTOR INDEX book_description_vector IF NOT EXISTS
        FOR (b:Book) ON (b.description_embedding)
        OPTIONS {indexConfig: {
          `vector.dimensions`: 768,
          `vector.similarity_function`: 'cosine'
        }}
        """

        try:
            session.run(query)
            logger.success("Created vector index for GraphRAG")
        except Exception as e: # pylint: disable=W0718
            logger.error(f"Vector index creation failed: {e}")


def index_aura():
    """Main indexing orchestrator."""
    driver = connect_auradb()

    try:
        create_aura_indexes(driver)
        create_vector_index(driver)
        logger.success("AuraDB indexes verified/created")
    finally:
        driver.close()


if __name__ == "__main__":
    index_aura()
