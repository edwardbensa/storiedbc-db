"""Create TTL indexes for ETL logs and batch summaries."""

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

    finally:
        close_mongodb()


if __name__ == "__main__":
    create_ttl_indexes()
