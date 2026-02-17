"""Loads transformed JSON collections into MongoDB"""

# Import modules
import time
import json
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm
from loguru import logger
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError
from src.utils.connectors import connect_mongodb, close_mongodb, retry
from src.utils.ops_mongo import load_sync_state, update_sync_state, sync_deletions
from src.utils.parsers import convert_document
from src.config import MAIN_COLL_DIR


class LoadMongoPipeline:
    """Pipeline for loading transformed JSON collections into MongoDB."""

    def __init__(self, directory=None, workers=8, dry_run=False):
        self.db = connect_mongodb()
        self.directory = Path(directory or MAIN_COLL_DIR)
        self.dry_run = dry_run
        self.workers = workers
        self.timestamp = datetime.now(timezone.utc)
        self.batch_id = time.strftime("%Y%m%d-%H%M%S")
        self.lst = load_sync_state(connect_mongodb("etl_metadata"), "main_sync")

    # Core load operations
    def load_file(self, file_path):
        """Load and clean a single JSON file."""
        try:
            with file_path.open(encoding="utf-8") as f:
                raw_docs = json.load(f)

            if isinstance(raw_docs, dict):
                raw_docs = [raw_docs]

            cleaned = [convert_document(doc) for doc in raw_docs]
            cleaned = [{**doc, "updated_at": self.timestamp} for doc in cleaned] # type: ignore
            return cleaned

        except (json.JSONDecodeError, FileNotFoundError, TypeError, ValueError) as e:
            logger.exception(f"Failed to parse '{file_path.name}': {e}")
            return None

    # Persistent logging
    def log_event(self, log_type, message, stats, context=None):
        """Insert a structured log entry into etl_metadata.logs."""
        try:
            log_doc = {
                "timestamp": time.time(),
                "pipeline": "load_mongo",
                "type": log_type,
                "message": message,
                "inserted": stats.get("inserted", 0),
                "updated": stats.get("updated", 0),
                "removed": stats.get("removed", 0),
                "duration_ms": stats.get("duration_ms", 0),
                "retry_count": stats.get("retry_count", 0),
                "context": context or {}
            }
            metadata_db = self.db.client["etl_metadata"]
            metadata_db["logs"].insert_one(log_doc)

        except PyMongoError as e:
            logger.error(f"Failed to write ETL log: {e}")

    def log_error(self, sheet, exc):
        """Insert an error log entry into etl_metadata.logs."""
        try:
            log_doc = {
                "timestamp": time.time(),
                "pipeline": "load_mongo",
                "type": "error",
                "batch_id": self.batch_id,
                "sheet": sheet,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "stack_trace": traceback.format_exc(),
                "context": {
                    "source": "system",
                    "target": "main"
                }
            }

            metadata_db = self.db.client["etl_metadata"]
            metadata_db["logs"].insert_one(log_doc)

        except PyMongoError as e:
            logger.error(f"Failed to write ETL error log: {e}")


    @retry(max_attempts=3, backoff=2)
    def upsert_collection(self, name, docs):
        """Bulk upsert documents into MongoDB with retry logic."""
        start = time.time()
        stats = {"inserted": 0, "updated": 0, "removed": 0}
        success = False

        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would upsert {len(docs)} docs into '{name}'")
                return stats

            ops = [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in docs]
            if not ops:
                return stats

            result = self.db[name].bulk_write(ops, ordered=False)

            stats["inserted"] = result.upserted_count
            stats["updated"] = result.modified_count
            success = True
            return stats

        except (KeyError, TypeError, BulkWriteError) as exc:
            logger.error(f"{name}: unrecoverable write error: {exc}")
            self.log_error(name, exc)
            return stats

        except Exception as exc:  # pylint: disable=W0718
            logger.error(f"{name}: unexpected error: {exc}")
            self.log_error(name, exc)
            return stats

        finally:
            stats["duration_ms"] = int((time.time() - start) * 1000)
            stats["retry_count"] = stats.pop("_retry_count", 0)

            if success:
                self.log_event(
                    log_type="upsert",
                    message=f"Loaded collection '{name}'",
                    stats=stats,
                    context={"collection": name}
                )


    # Single collection load
    def load_collection(self, file_path):
        """Load a single transformed collection into MongoDB."""
        name = file_path.stem
        logger.info(f"Loading '{name}'...")

        docs = self.load_file(file_path)
        if not docs:
            return {"inserted": 0, "updated": 0, "removed": 0, "retry_count": 0}

        return self.upsert_collection(name, docs)


    # Batch load (parallel)
    def load_all(self):
        """Load all JSON collections in parallel."""
        start = time.time()
        results = []

        json_files = list(self.directory.glob("*.json"))
        if not json_files:
            logger.warning("No JSON files found to load.")
            return

        logger.info(f"Found {len(json_files)} collections to load.")

        with tqdm(total=len(json_files), desc="Loading collections", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {executor.submit(self.load_collection, fp): fp for fp in json_files}

                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)

        summary = {
            "timestamp": time.time(),
            "pipeline": "load_mongo",
            "type": "batch_summary",
            "collections": [fp.stem for fp in json_files],
            "total_collections": len(json_files),
            "total_inserted": sum(r["inserted"] for r in results),
            "total_updated": sum(r["updated"] for r in results),
            "total_removed": sum(r["removed"] for r in results),
            "total_retries": sum(r.get("retry_count", 0) for r in results),
            "duration_ms": int((time.time() - start) * 1000)
        }

        metadata_db = self.db.client["etl_metadata"]
        metadata_db["batch_summaries"].insert_one(summary)
        update_sync_state(metadata_db, "main_sync", self.timestamp, self.batch_id)

        logger.success("All transformed collections loaded into MongoDB.")

    @retry(max_attempts=3, backoff=2)
    def sync_deletions(self):
        """Sync staging deletions with main."""
        sync_deletions(staging_db=connect_mongodb("staging"), main_db=self.db, since=self.lst)

    # Cleanup
    def close(self):
        """Close MongoDB client."""
        close_mongodb()


# Run
if __name__ == "__main__":
    pipeline = LoadMongoPipeline(workers=8)
    pipeline.load_all()
    pipeline.sync_deletions()
    pipeline.close()
