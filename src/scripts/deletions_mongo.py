from src.utils.ops_mongo import sync_deletions
from src.utils.connectors import connect_mongodb, close_mongodb
from src.utils.parsers import to_datetime

staging_db = connect_mongodb("staging")
main_db = connect_mongodb()

dt = to_datetime("2026-01-01")

sync_deletions(staging_db=staging_db, main_db=main_db, since=dt)

close_mongodb()
