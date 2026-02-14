from src.utils.ops_aura import sync_deletions
from src.utils.connectors import connect_auradb, connect_mongodb, close_mongodb
from src.utils.parsers import to_datetime

neo4j_driver = connect_auradb()
db = connect_mongodb()

dt = to_datetime("2026-01-01")

sync_deletions(neo4j_driver, db, dt)

close_mongodb()
