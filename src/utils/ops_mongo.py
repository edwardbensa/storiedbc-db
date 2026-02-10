"""Routine MongoDB operations"""

# Imports
import os
import json
from datetime import datetime
from loguru import logger
from bson import ObjectId
from pymongo import UpdateOne
from .connectors import close_mongodb
from .parsers import safe_value, flatten_document


def archive_delete(db, collection_name, filter_query):
    """Archive doc in deletions collection and delete from original."""
    source = db[collection_name]
    deletions = db["deletions"]

    # Ensure _id in filter_query is ObjectId
    if "_id" in filter_query and isinstance(filter_query["_id"], str):
        filter_query["_id"] = ObjectId(filter_query["_id"])

    # Find document
    doc = source.find_one(filter_query)
    if not doc:
        return {"deleted": False, "archived": False, "reason": "Document not found"}

    # Prepare archived version
    archived_doc = {
        **doc,
        "original_collection": collection_name,
        "deleted_at": datetime.now()
    }

    # Insert into deletions collection
    deletions.insert_one(archived_doc)

    # Delete from original collection
    result = source.delete_one({"_id": doc["_id"]})

    return {
        "deleted": result.deleted_count == 1,
        "archived": True,
        "id": str(doc["_id"])
    }

def drop_all_collections(db):
    """Drop all existing collections"""
    for name in db.list_collection_names():
        db.drop_collection(name)
        logger.info(f"Dropped collection '{name}'")

    close_mongodb()


def upsert_documents(db, collection_name, documents, timestamp=None, key="_id"):
    """Bulk upsert multiple documents into a MongoDB collection."""
    if collection_name not in db.list_collection_names():
        logger.info(f"Collection '{collection_name}' does not exist yet. Creating...")

    collection = db[collection_name]
    ops = []

    # Use provided timestamp or current time as fallback
    final_ts = timestamp or datetime.now()

    for doc in documents:
        if key not in doc:
            raise ValueError(f"Document missing required key '{key}': {doc}")

        # Ensure query with ObjectId if the key is _id to avoid "not found" errors
        query_val = doc[key]
        if key == "_id" and isinstance(query_val, str):
            query_val = ObjectId(query_val)

        # Apply the timestamp to the document
        doc["updated_at"] = final_ts

        update_payload = {k: v for k, v in doc.items() if k != key}

        ops.append(UpdateOne({key: query_val}, {"$set": update_payload}, upsert=True))

    if ops:
        result = collection.bulk_write(ops, ordered=False)
        logger.success(
            f"Bulk upsert complete for '{collection_name}': "
            f"{result.upserted_count} inserted, {result.modified_count} updated."
        )

def fetch_documents(collection, exclude_fields=None, field_map=None,
                    since=None, flatten=True, query=None) -> list:
    """
    Fetch documents updated since 'since' with field exclusions and flattening.
    """
    if exclude_fields is None:
        exclude_fields = []
    if field_map is None:
        field_map = {}
    else:
        flatten = True

    # Build query
    query = query.copy() if query else {}

    if since is not None:
        query.setdefault("updated_at", {})
        query["updated_at"]["$gt"] = since

    projection = {field: 0 for field in exclude_fields}

    docs = list(collection.find(query, projection))
    #docs = [{k: safe_value(v) for k, v in doc.items()} for doc in docs]

    if flatten:
        flattened = []
        for doc in docs:
            flat = flatten_document(doc, field_map)
            flattened.append(flat)

        logger.success(f"Fetched {len(flattened)} documents from {collection.name} collection.")
        return flattened

    return docs

def download_collections(db, output_dir, excluded_collections: list, since: datetime):
    """
    Downloads all collections (except excluded) since specified timestamp as JSON.
    Returns the count of collections that actually had new data.
    """
    os.makedirs(output_dir, exist_ok=True)

    collections = db.list_collection_names()
    files_created = 0

    for collection_name in collections:
        if collection_name in excluded_collections:
            logger.info(f"Skipping excluded collection: {collection_name}")
            continue

        collection = db[collection_name]
        documents = fetch_documents(collection, since=since, flatten=False)
        documents = [{k: safe_value(v) for k, v in doc.items()} for doc in documents]

        # Write to file
        if documents:
            file_path = os.path.join(output_dir, f"{collection_name}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(documents, f, indent=2)

            logger.info(f"Downloaded delta for {collection_name} ({len(documents)} records)")
            files_created += 1

    logger.success("Downloads complete.")
    return files_created

def load_sync_state(etl_db, _id: str):
    """Load sync state (last sync time) from MongoDB."""
    doc = etl_db.sync_states.find_one({"_id": _id})
    if doc and "last_sync_time" in doc:
        return doc["last_sync_time"]
    return datetime(2026, 1, 1)

def update_sync_state(etl_db, _id, timestamp, batch_id):
    """Update sync state in MongoDB."""
    etl_db.sync_states.update_one(
        {"_id": _id},
        {"$set": {
            "last_sync_time": timestamp,
            "last_batch_id": batch_id
        }},
        upsert=True
    )
