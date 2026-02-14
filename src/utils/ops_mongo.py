"""Routine MongoDB operations"""

# Imports
import os
import json
from datetime import datetime, timezone
from loguru import logger
from bson import ObjectId
from pymongo import UpdateOne
from .parsers import safe_value, flatten_document


def archive_delete(db, collection_name, filter_query):
    """Archive document in deletions collection and delete from original."""
    source = db[collection_name]
    deletions = db["deletions"]

    try:
        # Ensure _id in filter_query is ObjectId
        query = filter_query.copy()
        if "_id" in query and isinstance(query["_id"], str):
            query["_id"] = ObjectId(query["_id"])

        # Find document
        doc = source.find_one(query)
        if not doc:
            return {"deleted": False, "archived": False, "reason": "Document not found"}

        # Prepare archived version
        original_id = doc.pop("_id")
        archived_doc = {
            "_id": ObjectId(),
            **doc,
            "original_id": original_id,
            "original_collection": collection_name,
            "deleted_at": datetime.now(timezone.utc)
            }

        # Insert into deletions collection
        deletions.insert_one(archived_doc)

        # Delete from original collection
        result = source.delete_one({"_id": doc["original_id"]})

        return {
            "deleted": result.deleted_count == 1,
            "archived": True,
            "archived_id": str(archived_doc["_id"]),
            "original_id": str(original_id)
        }
    except Exception as e: # pylint: disable=W0718
        logger.exception(
            f"archive_delete: Error archiving/deleting from {collection_name} "
            f"with query={filter_query}: {e}"
            )
        return {
            "deleted": False,
            "archived": False,
            "error": str(e)
            }


def sync_deletions(main_db, staging_db, since):
    """Sync deletion records from staging into main DB."""
    try:
        # Ensure timestamp is timezone-aware
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        query = {"deleted_at": {"$gt": since}}
        logger.debug(f"sync_deletions: Fetching staging deletions since {since}")

        deletions = list(staging_db["deletions"].find(query))

        if not deletions:
            logger.info("sync_deletions: No new deletions to sync.")
            return {"synced": 0}

        synced_count = 0

        for doc in deletions:
            try:
                original_id = doc.get("original_id")
                collection_name = doc.get("original_collection")

                if not original_id or not collection_name:
                    logger.warning(f"Skipping malformed deletion record: {doc}")
                    continue

                logger.debug(f"Syncing {original_id} deletion from collection '{collection_name}")

                result = archive_delete(main_db, collection_name, {"_id": original_id})

                if result.get("deleted"):
                    synced_count += 1
                else:
                    logger.warning(f"Could not delete {original_id}. in main DB: {result}")

            except Exception as inner_err: # pylint: disable=W0718
                logger.exception(f"Error syncing deletion record {doc}: {inner_err}")

        logger.info(f"Completed. Synced {synced_count} deletions.")

        return {"synced": synced_count}

    except Exception as e: # pylint: disable=W0718
        logger.exception(f"Fatal error during sync: {e}")
        return {"synced": 0, "error": str(e)}


def drop_all_collections(db):
    """Drop all existing collections"""
    for name in db.list_collection_names():
        db.drop_collection(name)
        logger.info(f"Dropped collection '{name}'")


def upsert_documents(db, collection_name, documents, timestamp=None,
                     key="_id", batch_size=1000):
    """Bulk upsert into a MongoDB collection with chunking."""
    if collection_name not in db.list_collection_names():
        logger.info(f"Collection '{collection_name}' does not exist yet. Creating...")

    collection = db[collection_name]
    final_ts = timestamp or datetime.now(timezone.utc)

    total_inserted = 0
    total_updated = 0

    # Process in chunks
    for i in range(0, len(documents), batch_size):
        chunk = documents[i:i + batch_size]
        ops = []

        for doc in chunk:
            if key not in doc:
                raise ValueError(f"Document missing required key '{key}': {doc}")

            query_val = doc[key]
            if key == "_id" and isinstance(query_val, str):
                query_val = ObjectId(query_val)

            doc["updated_at"] = final_ts
            update_payload = {k: v for k, v in doc.items() if k != key}
            ops.append(UpdateOne({key: query_val}, {"$set": update_payload}, upsert=True))

        if ops:
            result = collection.bulk_write(ops, ordered=False)
            total_inserted += result.upserted_count
            total_updated += result.modified_count

    logger.success(
        f"Bulk upsert complete for '{collection_name}': "
        f"{total_inserted} inserted, {total_updated} updated."
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
    return datetime(2026, 1, 1, tzinfo=timezone.utc)


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
