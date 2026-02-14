"""Migrate inventory to new structure."""

# Imports
import os
from urllib.parse import urlparse
from loguru import logger
from src.utils.connectors import connect_mongodb, close_mongodb
from src.utils.ops_mongo import fetch_documents, update_inventory
from src.utils.files import generate_image_filename

etl_db = connect_mongodb("etl_metadata")
staging_db = connect_mongodb("staging")

source_collection = "book_versions"
data = fetch_documents(staging_db[source_collection])
img_type = "cover"

for doc in data:
    url_field = "cover_url"
    image_url = doc.get(url_field)
    if not image_url:
        logger.warning(f"Entry with _id {doc.get('_id')} has no {url_field}. Skipping.")
        continue

    parsed_url = urlparse(image_url)
    extension = os.path.splitext(parsed_url.path)[1] or ".jpg"
    filename = f"{generate_image_filename(doc, img_type)}{extension}"

    update_inventory(etl_db, img_type, doc, filename, url_field, source_collection)

close_mongodb()
