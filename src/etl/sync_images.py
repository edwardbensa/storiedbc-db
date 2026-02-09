"""Sync images with Azure Blob Storage"""

# Imports
import os
import json
from urllib.parse import urlparse
from loguru import logger
from src.utils.connectors import connect_mongodb, close_mongodb, connect_azure_blob, sync_images
from src.utils.files import generate_image_filename
from src.config import STAGING_COLL_DIR


# Connect to MongoDB and Azure Blob Storage
db = connect_mongodb("etl_metadata")
blob_service_client = connect_azure_blob()

def run_image_sync(collection, url_field, img_type, container_name):
    """Sync images."""
    # Load your processed data
    with open(STAGING_COLL_DIR / f"{collection}.json", "r", encoding="utf-8") as f:
        versions = json.load(f)

    # Build manifest: {hashed_filename: original_url}
    manifest = {}
    for doc in versions:
        image_url = doc.get(url_field)
        if not image_url:
            logger.warning(f"Entry with _id {doc.get('_id')} has no {url_field}. Skipping.")
            continue

        parsed_url = urlparse(image_url)
        extension = os.path.splitext(parsed_url.path)[1] or ".jpg"
        filename = f"{generate_image_filename(doc, img_type)}{extension}"
        manifest[filename] = doc[url_field]

    # Sync directly to Azure
    sync_images(db, blob_service_client, container_name, img_type, manifest)
    logger.success("Images successfully synced to Azure containers.")


if __name__ == "__main__":
    run_image_sync("book_versions", "cover_url", "cover", "cover_art")
    close_mongodb()
