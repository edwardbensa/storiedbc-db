"""One-time database setup: create indexes, constraints, TTL policies."""

import sys
from loguru import logger
from src.scripts.index_mongo import index_mongo
from src.scripts.index_aura import index_aura


def setup_all():
    """Run all database setup operations."""
    logger.info("=" * 60)
    logger.info("DATABASE SETUP")
    logger.info("=" * 60)

    try:
        logger.info("\n[1/2] Setting up MongoDB...")
        index_mongo()

        logger.info("\n[2/2] Setting up AuraDB...")
        index_aura()

        logger.success("\n" + "=" * 60)
        logger.success("DATABASE SETUP COMPLETE")
        logger.success("=" * 60)

    except Exception as e: # pylint: disable=W0718
        logger.error(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    setup_all()
