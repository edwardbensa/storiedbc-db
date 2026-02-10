"""Transform new staging data for upload to main database."""

# Imports
import os
import sys
from pathlib import Path
from loguru import logger
from src.utils.ops_mongo import download_collections, load_sync_state, fetch_documents
from src.utils.connectors import connect_azure_blob, connect_mongodb, close_mongodb
from src.utils.transform_for_main import (
    build_registries, transform_collection, remove_custom_ids, set_custom_ids,
    transform_map, cleanup_map, custom_id_map
    )
from src.config import STAGING_COLL_DIR, MAIN_COLL_DIR


if __name__ == "__main__":
    # Create directories
    logger.info("Creating staging and main collection delta folders..")
    os.makedirs(STAGING_COLL_DIR, exist_ok=True)
    os.makedirs(MAIN_COLL_DIR, exist_ok=True)

    # Connect to databases
    main_db = connect_mongodb("main")
    staging_db = connect_mongodb("staging")
    etl_db = connect_mongodb("etl_metadata")

    # Load sync state
    lst = load_sync_state(etl_db, "main_sync")

    # Download deltas and capture count
    logger.info("Fetching collections...")
    DELTA_COUNT = download_collections(staging_db, STAGING_COLL_DIR, ["deletions"], lst)

    # Early exit signal
    if DELTA_COUNT == 0:
        logger.info("No new data found in staging. Signalling early pipeline stop.")
        sys.exit(10) # Early stop. Success, but nothing to process

    # Blob Service Client
    blob_acc = connect_azure_blob().account_name

    # Load files
    books = fetch_documents(collection=staging_db["books"],
            query={"series": {"$ne": ""}}, flatten=False)
    user_reads_updates = fetch_documents(staging_db["user_reads"], since=lst)
    version_ids = {doc["version_id"] for doc in user_reads_updates}
    book_versions = fetch_documents(collection=staging_db["book_versions"],
            query={"version_id": {"$in": list(version_ids)}}, flatten=False)

    # Create lookup and subdocument registries
    lookup_maps, subdoc_registry = build_registries(db=staging_db)
    logger.info("Lookup and subdocument registries created.")

    # Close database
    close_mongodb()

    # Create context
    context = {
        "lookup_data": lookup_maps,
        "subdoc_registry": subdoc_registry,
        "books": books,
        "book_versions": book_versions,
        "blob_account": blob_acc,
    }

    delta_files = [f.stem for f in Path(STAGING_COLL_DIR).glob("*.json")]

    if not delta_files:
        logger.info("No new deltas found. Transformation skipped.")
    else:
        logger.info(f"Dynamically processing {len(delta_files)} collections...")
        for collection in delta_files:
            if collection in transform_map:
                logger.info(f"Transforming: {collection}")
                transform_collection(collection, transform_map[collection], context=context)

    remove_custom_ids(cleanup_map, STAGING_COLL_DIR)
    set_custom_ids(custom_id_map)
    logger.info("Cleaned collections.")
