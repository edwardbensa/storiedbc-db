"""
Create MongoDB indexes for optimal query performance.
Create TTL indexes for ETL logs and batch summaries.
"""

from loguru import logger
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from src.utils.connectors import connect_mongodb, close_mongodb


LOG_TTL_DAYS = 60
SUMMARY_TTL_DAYS = 90

def create_ttl_indexes():
    """Create time to live index to automatically remove log docs."""
    etl_db = connect_mongodb("etl_metadata")

    try:
        # Short retention
        etl_db["logs"].create_index(
            [("timestamp", ASCENDING)],
            expireAfterSeconds=LOG_TTL_DAYS * 24 * 3600,
            name="ttl_logs_60d"
        )

        # Long retention
        etl_db["batch_summaries"].create_index(
            [("timestamp", ASCENDING)],
            expireAfterSeconds=SUMMARY_TTL_DAYS * 24 * 3600,
            name="ttl_batch_summaries_90d"
        )

        logger.info("TTL indexes created successfully.")

    except PyMongoError as e:
        logger.info(f"Failed to create TTL indexes: {e}")


def create_mongo_indexes():
    """Create all necessary MongoDB indexes."""
    db = connect_mongodb("main")

    indexes = {
        "books": [
            ("title", "text"),
            ("author.name", 1),
            ("genre", 1),
            ("publication_year", -1),
            ("updated_at", -1),
        ],
        "book_versions": [
            ("book_id", 1),
            ("isbn_13", 1),
            ("asin", 1),
            ("updated_at", -1),
        ],
        "users": [
            ("handle", 1),
            ("email_address", 1),
            ("updated_at", -1),
        ],
        "clubs": [
            ("handle", 1),
            ("created_at", -1),
            ("updated_at", -1),
        ],
        "user_reads": [
            ("user_id", 1),
            ("version_id", 1),
            ("most_recent_rstatus", 1),
            ("updated_at", -1),
        ],
        "deletions": [
            ("deleted_at", -1),
            ("original_collection", 1),
        ],
    }

    for collection, index_specs in indexes.items():
        existing = db[collection].index_information()

        for spec in index_specs:
            if isinstance(spec, tuple):
                field, direction = spec
                index_name = f"{field.replace('.', '_')}_{direction}"
            else:
                # Compound index
                index_name = "_".join(f"{f}_{d}" for f, d in spec)

            if index_name not in existing:
                db[collection].create_index([spec])
                logger.info(f"Created index on {collection}.{index_name}")
            else:
                logger.debug(f"Index {collection}.{index_name} already exists")

    logger.success("MongoDB indexes verified/created")


def index_mongo():
    """Main indexing orchestrator."""
    try:
        create_ttl_indexes()
        create_mongo_indexes()
        logger.success("Main MongoDB indexes and TTL indexes verified/created")
    finally:
        close_mongodb()


if __name__ == "__main__":
    index_mongo()
