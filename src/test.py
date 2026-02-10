from src.utils.ops_mongo import fetch_documents, load_sync_state
from src.utils.transform_for_main import build_registries
from src.utils.connectors import connect_mongodb

staging_db = connect_mongodb("staging")
etl_db = connect_mongodb("etl_metadata")
lst = load_sync_state(etl_db, "main_sync")

books = fetch_documents(collection=staging_db["books"],
                        query={"series": {"$ne": ""}}, flatten=False)
user_reads_updates = fetch_documents(staging_db["user_reads"], since=lst)
version_ids = {doc["version_id"] for doc in user_reads_updates}
book_versions = fetch_documents(
    collection=staging_db["book_versions"],
    query={"version_id": {"$in": list(version_ids)}}, flatten=False)


lookup_maps, subdoc_registry = build_registries(db=staging_db)

print(lookup_maps)
