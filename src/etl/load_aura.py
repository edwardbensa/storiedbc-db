"""Load staged AuraDB JSON collections into Neo4j."""

import json
import time
import datetime
from pathlib import Path
from loguru import logger
from pymongo.errors import PyMongoError
from src.utils.ops_mongo import load_sync_state, update_sync_state
from src.utils.ops_aura import (
    upsert_nodes, ensure_constraints, create_relationships, user_reads_relationships,
    badges_relationships, book_awards_relationships, club_book_relationships,
    cleanup_nodes, sync_deletions, deduplicate_nodes, get_relationships
    )
from src.utils.connectors import (
    connect_mongodb, close_mongodb, connect_auradb, retry, RETRYABLE_ERRORS
    )
from src.utils.transform_for_aura import collection_label_map
from src.config import AURA_COLL_DIR


class AuraSyncPipeline:
    """Pipeline for loading staged JSON data into AuraDB."""

    SYNC_KEY = "auradb_sync"

    def __init__(self):
        # Connections
        self.etl_db = connect_mongodb("etl_metadata")
        self.db = connect_mongodb()
        self.neo4j_driver = connect_auradb()

        # Batch metadata
        self.timestamp = datetime.datetime.now(datetime.timezone.utc)
        self.batch_id = time.strftime("%Y%m%d-%H%M%S")
        self.lst = load_sync_state(self.etl_db, self.SYNC_KEY)

        # Directory with staged JSON files
        self.input_dir = Path(AURA_COLL_DIR)

    # Logging helpers
    def log_event(self, log_type, message, stats, context=None):
        """Insert a structured log entry into etl_metadata.logs."""
        try:
            log_doc = {
                "timestamp": time.time(),
                "pipeline": "auradb_sync",
                "type": log_type,
                "message": message,
                "inserted": stats.get("inserted", 0),
                "updated": stats.get("updated", 0),
                "removed": stats.get("removed", 0),
                "duration_ms": stats.get("duration_ms", 0),
                "retry_count": stats.get("retry_count", 0),
                "context": {
                    "batch_id": self.batch_id,
                    **(context or {}),
                },
            }
            self.etl_db["logs"].insert_one(log_doc)
        except PyMongoError as e:
            logger.error(f"Failed to write ETL log: {e}")

    def log_error(self, stage, exc, extra_context=None):
        """Insert an error log entry into etl_metadata.logs."""
        try:
            log_doc = {
                "timestamp": time.time(),
                "pipeline": "auradb_sync",
                "type": "error",
                "batch_id": self.batch_id,
                "stage": stage,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "stack_trace": getattr(exc, "__traceback__", None),
                "context": extra_context or {},
            }
            self.etl_db["logs"].insert_one(log_doc)
        except PyMongoError as e:
            logger.error(f"Failed to write ETL error log: {e}")

    # Core helpers

    def load_json(self, name):
        """Load a JSON file from the staging directory."""
        file_path = self.input_dir / f"{name}.json"
        if not file_path.exists():
            logger.warning(f"Missing staged file: {file_path}")
            return []

        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def load_staged_data(self):
        """Load all staged JSON files into memory."""
        collections = [
            "books",
            "book_versions",
            "book_series",
            "genres",
            "awards",
            "creators",
            "creator_roles",
            "publishers",
            "formats",
            "languages",
            "users",
            "clubs",
            "user_badges",
            "club_badges",
            "countries",
            "user_reads",
            "book_awards",
            "club_period_books",
        ]

        data = {name: self.load_json(name) for name in collections}
        return data

    @retry(max_attempts=3, backoff=2)
    def load_nodes(self, nodes):
        """Upsert all node types into AuraDB with per-node logs."""
        start = time.time()
        total_processed = 0
        retry_count = 0

        try:
            with self.neo4j_driver.session() as session:
                for label, rows in nodes.items():
                    if not rows:
                        continue

                    node_start = time.time()
                    try:
                        session.execute_write(upsert_nodes, label, rows)
                        items = len(rows)
                        total_processed += items

                        node_stats = {
                            "inserted": items,
                            "updated": 0,
                            "removed": 0,
                            "duration_ms": int((time.time() - node_start) * 1000),
                            "retry_count": 0,
                        }
                        self.log_event(
                            "upsert_nodes",
                            f"Upserted nodes for label '{label}'",
                            node_stats,
                            context={"label": label},
                        )
                    except Exception as exc: # pylint: disable=W0718
                        self.log_error("load_nodes", exc, extra_context={"label": label})

            return {"inserted": total_processed}

        finally:
            duration = int((time.time() - start) * 1000)
            stats = {
                "inserted": total_processed,
                "duration_ms": duration,
                "retry_count": retry_count,
            }
            self.log_event("load_nodes", "Completed node upserts", stats)

    @retry(max_attempts=3, backoff=2)
    def load_relationships(self, data):
        """Create all relationships in AuraDB (grouped log)."""
        start = time.time()
        retry_count = 0

        try:
            with self.neo4j_driver.session() as session:
                # Relationship maps
                rel_maps = get_relationships(data)

                for rel_map, rel_type, source_docs in rel_maps:
                    try:
                        session.execute_write(create_relationships, rel_map, rel_type, source_docs)
                    except RETRYABLE_ERRORS:
                        raise
                    except Exception as exc: # pylint: disable=W0718
                        self.log_error("load_relationships", exc, extra_context={"rel_type": rel_type}) # pylint: disable=C0301

                # Additional relationship functions
                if data.get("user_reads", None):
                    session.execute_write(user_reads_relationships, data["user_reads"])
                if data.get("users", None):
                    session.execute_write(badges_relationships, data["users"], "User")
                if data.get("clubs", None):
                    session.execute_write(badges_relationships, data["clubs"], "Club")
                if data.get("book_awards", None):
                    session.execute_write(book_awards_relationships, data["books"], data["book_awards"])
                if data.get("club_period_books", None):
                    session.execute_write(club_book_relationships, data["club_period_books"])

            return {"inserted": 0}

        finally:
            duration = int((time.time() - start) * 1000)
            stats = {"inserted": 0, "duration_ms": duration, "retry_count": retry_count}
            self.log_event("relationships", "Created relationships in AuraDB", stats)

    @retry(max_attempts=3, backoff=2)
    def run_cleanup(self):
        """Cleanup temporary properties from nodes."""
        cleanup_map = {
            "Book": ["author_id", "series_id"],
            "BookVersion": [
                "book_id",
                "publisher_id",
                "narrator_id",
                "illustrator_id",
                "translator_id",
                "cover_artist_id",
            ],
            "User": ["club_ids", "badge_timestamps"],
            "Club": ["created_by"],
        }

        start = time.time()
        retry_count = 0

        try:
            cleanup_nodes(self.neo4j_driver, cleanup_map)
            return {"inserted": 0}
        except Exception as exc: # pylint: disable=W0718
            self.log_error("cleanup", exc)
            return {"inserted": 0}
        finally:
            duration = int((time.time() - start) * 1000)
            stats = {"inserted": 0, "duration_ms": duration, "retry_count": retry_count}
            self.log_event("cleanup", "Cleaned up temporary properties", stats)

    def ensure_constraints(self):
        """Ensure Neo4j constraints exist."""
        constraints_map = {
            "User": "_id",
            "Club": "_id",
            "Book": "_id",
            "BookVersion": "_id",
            "Award": "_id",
            "Creator": "_id",
            "UserBadge": "name",
            "ClubBadge": "name",
            "Genre": "name",
            "Country": "name",
            "Format": "name",
            "Language": "name",
        }
        ensure_constraints(self.neo4j_driver, constraints_map)

    def update_sync_state(self):
        """Update sync state in MongoDB."""
        update_sync_state(self.etl_db, self.SYNC_KEY, self.timestamp, self.batch_id)

    def close(self):
        """Close connections."""
        try:
            self.neo4j_driver.close()
        except Exception: # pylint: disable=W0718
            pass
        close_mongodb()

    # Orchestrator

    def sync_all(self):
        """Run the full AuraDB sync pipeline."""
        batch_start = time.time()

        # Load staged JSON
        data = self.load_staged_data()

        # Constraints
        self.ensure_constraints()

        # Sync deletions
        sync_deletions(self.neo4j_driver, self.db, self.lst)

        # Load nodes
        node_stats = self.load_nodes({
            "Book": data["books"],
            "BookVersion": data["book_versions"],
            "BookSeries": data["book_series"],
            "Genre": data["genres"],
            "Award": data["awards"],
            "Creator": data["creators"],
            "CreatorRole": data["creator_roles"],
            "Publisher": data["publishers"],
            "Format": data["formats"],
            "Language": data["languages"],
            "User": data["users"],
            "Club": data["clubs"],
            "UserBadge": data["user_badges"],
            "ClubBadge": data["club_badges"],
            "Country": data["countries"],
        })

        # Load relationships
        rel_stats = self.load_relationships(data)

        # Cleanup
        cleanup_stats = self.run_cleanup()
        labels = list(collection_label_map.values())
        for label in labels:
            deduplicate_nodes(self.neo4j_driver, label)

        # Update sync state
        self.update_sync_state()

        # Batch summary
        duration_ms = int((time.time() - batch_start) * 1000)
        summary = {
            "timestamp": time.time(),
            "pipeline": "auradb_sync",
            "type": "batch_summary",
            "batch_id": self.batch_id,
            "total_nodes": node_stats.get("inserted", 0),
            "total_relationships": 0,
            "total_retries": (
                node_stats.get("retry_count", 0)
                + rel_stats.get("retry_count", 0)
                + cleanup_stats.get("retry_count", 0)
            ),
            "duration_ms": duration_ms,
        }

        try:
            self.etl_db["batch_summaries"].insert_one(summary)
        except PyMongoError as e:
            logger.error(f"Failed to write batch summary: {e}")

        logger.success("AuraDB sync pipeline completed successfully.")
        return summary


# Run
if __name__ == "__main__":
    pipeline = AuraSyncPipeline()
    try:
        pipeline.sync_all()
    finally:
        pipeline.close()
