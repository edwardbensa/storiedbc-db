"""Sync images with Azure Blob Storage"""

# Imports
import os
import json
from urllib.parse import urlparse
from loguru import logger
from src.utils.connectors import connect_mongodb, close_mongodb, connect_azure_blob, sync_images
from src.utils.files import generate_image_filename
from src.utils.ops_mongo import update_inventory
from src.config import STAGING_COLL_DIR


# Connect to MongoDB and Azure Blob Storage
etl_db = connect_mongodb("etl_metadata")
blob_service_client = connect_azure_blob()

def run_image_sync(collection, url_field, img_type, container_name):
    """Update image inventory and sync images to Azure."""
    # Load data
    with open(STAGING_COLL_DIR / f"{collection}.json", "r", encoding="utf-8") as f:
        new_docs = json.load(f)

    # Update image inventory
    for doc in new_docs:
        image_url = doc.get(url_field)
        if not image_url:
            logger.warning(f"Entry with _id {doc.get('_id')} has no {url_field}. Skipping.")
            continue

        parsed_url = urlparse(image_url)
        extension = os.path.splitext(parsed_url.path)[1] or ".jpg"
        filename = f"{generate_image_filename(doc, img_type)}{extension}"

        update_inventory(etl_db, img_type, doc, filename, url_field, collection)

    sync_images(etl_db, blob_service_client, img_type, container_name)
    logger.success("Images successfully synced to Azure containers.")


if __name__ == "__main__":
    run_image_sync("book_versions", "cover_url", "cover", "covers")
    close_mongodb()
