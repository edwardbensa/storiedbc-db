"""Google Sheets sync: updates, adds, and removes records while preserving ObjectIds"""

# Imports
import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from loguru import logger
from pymongo.errors import PyMongoError
from src.utils.parsers import add_hashes, find_deltas, id_docs
from src.utils.connectors import connect_gsheet, connect_mongodb, retry
from src.utils.mongo_ops import fetch_documents, upsert_documents, archive_delete


# Class-based pipeline
class SheetSyncPipeline:
    """Google Sheets sync pipeline for extracting and loading data into Book Club ETL db."""
    def __init__(self, db, spreadsheet, id_map, dry_run=False, workers=1): # pylint: disable=W0621
        self.db = db
        self.spreadsheet = spreadsheet
        self.id_map = id_map
        self.dry_run = dry_run
        self.workers = min(workers, 5)
        self.timestamp = datetime.now()
        self.batch_id = time.strftime("%Y%m%d-%H%M%S")

        self.avg_latency = 0
        self.alpha = 0.3  # smoothing factor for dynamic sleep

    # Dynamic sleep
    def dynamic_sleep(self, latency):
        """Dynamic sleep based on Sheets API latency"""
        self.avg_latency = self.alpha * latency + (1 - self.alpha) * self.avg_latency

        if self.avg_latency > 1.5:
            sleep_time = min(10, self.avg_latency * 2)
            logger.info(f"High API latency ({self.avg_latency:.2f}s). Sleeping {sleep_time:.1f}s.")
            time.sleep(sleep_time)
        else:
            logger.info(f"Latency normal ({self.avg_latency:.2f}s). No sleep needed.")

    # Persistent logging
    def log_event(self, log_type, message, stats, context=None):
        """Insert a structured log entry into etl_metadata.logs."""
        try:
            log_doc = {
                "timestamp": time.time(),
                "pipeline": "gsheet_sync",
                "type": log_type,
                "message": message,
                "inserted": stats["inserted"],
                "updated": stats["updated"],
                "removed": stats["removed"],
                "duration_ms": stats["duration_ms"],
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
                "pipeline": "gsheet_sync",
                "type": "error",
                "batch_id": self.batch_id,
                "sheet": sheet,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "stack_trace": traceback.format_exc(),
                "context": {
                    "source": "gsheet",
                    "target": "staging"
                }
            }

            metadata_db = self.db.client["etl_metadata"]
            metadata_db["logs"].insert_one(log_doc)

        except PyMongoError as e:
            logger.error(f"Failed to write ETL error log: {e}")


    # Core sync operations
    @retry(max_attempts=5, backoff=2)
    def load_sheet(self, name):
        """Load Google Sheet."""
        sheet = self.spreadsheet.worksheet(name)
        start = time.time()
        records = sheet.get_all_records()
        latency = time.time() - start
        self.dynamic_sleep(latency)
        return records

    def fetch_docs(self, name):
        """Fetch documents from MongoDB."""
        return fetch_documents(self.db[name])

    def apply_upserts(self, name, docs):
        """Upsert documents into MongoDB."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would upsert {len(docs)} docs into '{name}'")
            return
        upsert_documents(self.db, name, docs, timestamp=self.timestamp)

    def apply_deletions(self, name, removed):
        """Archive delete removed entries."""
        for entry in removed:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would archive+delete {entry['_id']} from '{name}'")
                continue

            result = archive_delete(self.db, name, {"_id": entry["_id"]})
            logger.info(
                f"Removed record {entry['_id']} from '{name}': "
                f"archived={result['archived']}, deleted={result['deleted']}"
            )

    # Sync one sheet
    @retry(max_attempts=3, backoff=2)
    def sync_sheet(self, name):
        """Extract and sync one sheet from Book Club DB Google Sheet."""
        start_time = time.time()
        logger.info(f"Syncing sheet '{name}'...")

        stats = {"inserted": 0, "updated": 0, "removed": 0}
        success = False

        try:
            new_list = self.load_sheet(name)
            new_list, _, new_fh = add_hashes(new_list, self.id_map[name])

            if not new_list:
                logger.warning(f"No records found in sheet '{name}'")
                return stats

            old_list = self.fetch_docs(name)
            logger.info(f"Found {len(old_list)} stored records for '{name}'.")

            if not old_list:
                logger.info(f"No stored records found for '{name}'. Saving as new.")
                new_list = id_docs(new_list)
                self.apply_upserts(name, new_list)
                stats["inserted"] = len(new_list)
                success = True
                return stats

            # Check for any changes in records
            old_fh = [doc["full_hash"] for doc in old_list if "full_hash" in doc]
            if len(new_list) == len(old_list) and set(new_fh) == set(old_fh):
                logger.info(f"'{name}' unchanged. Skipping.")
                success = True
                return stats

            # Calculate deltas
            to_upsert, diff = find_deltas(old_list, new_list)

            # Execute archive deletions
            self.apply_deletions(name, diff["deleted"])

            # Execute upserts of new + updated
            if to_upsert:
                self.apply_upserts(name, to_upsert)

            # Update stats
            stats["inserted"] = len(diff["new"])
            stats["updated"] = len(diff["updated"])
            stats["removed"] = len(diff["deleted"])

            success = True
            return stats

        except (KeyError, ValueError) as exc:
            logger.error(f"Error syncing sheet '{name}': {exc}")
            self.log_error(name, exc)
            return stats

        except Exception as exc: # pylint: disable=W0718
            logger.error(f"Unexpected error syncing sheet '{name}': {exc}")
            self.log_error(name, exc)
            return stats

        finally:
            elapsed = time.time() - start_time
            stats["duration_ms"] = int(elapsed * 1000)
            stats["retry_count"] = stats.pop("_retry_count", 0)

            context = {
                "sheet": name,
                "source": "gsheet",
                "target": "staging",
                "batch_id": self.batch_id,
            }

            if success:
                self.log_event(
                    log_type="upsert",
                    message=f"Synced sheet '{name}'",
                    stats=stats,
                    context=context
                )
            else:
                pass


    # Sync group (sequential or concurrent)
    def sync_group(self, sheet_names):
        """Sequentially or concurrently sync multiple sheets."""
        start = time.time()
        results = []

        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                for stats in tqdm(pool.map(self.sync_sheet, sheet_names),
                                total=len(sheet_names),
                                desc="Syncing sheets"):
                    results.append(stats)
        else:
            for name in tqdm(sheet_names, desc="Syncing sheets"):
                stats = self.sync_sheet(name)
                results.append(stats)

        # Batch summary
        elapsed = int((time.time() - start) * 1000)

        summary = {
            "timestamp": time.time(),
            "pipeline": "gsheet_sync",
            "type": "batch_summary",
            "batch_id": self.batch_id,
            "sheets": sheet_names,
            "total_sheets": len(sheet_names),
            "total_inserted": sum(r["inserted"] for r in results),
            "total_updated": sum(r["updated"] for r in results),
            "total_removed": sum(r["removed"] for r in results),
            "total_retries": sum(r.get("retry_count", 0) for r in results),
            "duration_ms": elapsed
        }

        metadata_db = self.db.client["etl_metadata"]
        metadata_db["batch_summaries"].insert_one(summary)


# Run
if __name__ == "__main__":
    spreadsheet = connect_gsheet()
    db = connect_mongodb("staging")

    id_map = {
        "books": ["title", "genre"],
        "book_versions": ["isbn_13", "asin"],
        "creators": ["firstname", "lastname"],
        "creator_roles": ["name"],
        "genres": ["name"],
        "book_series": ["name"],
        "awards": ["name"],
        "publishers": ["name"],
        "formats": ["name"],
        "tags": ["name"],
        "languages": ["name"],
        "users": ["handle"],
        "user_reads": ["ur_id"],
        "read_statuses": ["name"],
        "user_badges": ["name"],
        "user_roles": ["name"],
        "user_permissions": ["name"],
        "clubs": ["handle"],
        "club_members": ["club_id", "user_id"],
        "club_member_reads": ["cmr_id"],
        "club_reading_periods": ["club_id", "period_id"],
        "club_period_books": ["cpb_id"],
        "club_discussions": ["club_id", "discussion_id"],
        "club_events": ["event_id"],
        "club_event_types": ["name"],
        "club_event_statuses": ["name"],
        "club_badges": ["name"],
        "countries": ["name"]
    }

    pipeline = SheetSyncPipeline(
        db=db,
        spreadsheet=spreadsheet,
        id_map=id_map,
        dry_run=False,
        workers=4
    )

    logger.info("Syncing book sheets...")
    pipeline.sync_group([
        "books", "book_versions", "creators", "creator_roles", "genres",
        "book_series", "awards", "publishers", "formats", "tags", "languages"
    ])

    logger.info("Syncing user sheets...")
    pipeline.sync_group([
        "users", "user_reads", "read_statuses", "user_badges",
        "user_roles", "user_permissions"
    ])

    logger.info("Syncing club sheets...")
    pipeline.sync_group([
        "clubs", "club_members", "club_member_reads", "club_reading_periods",
        "club_period_books", "club_discussions", "club_events",
        "club_event_types", "club_event_statuses", "club_badges"
    ])

    logger.info("Syncing miscellaneous sheets...")
    pipeline.sync_group(["countries"])

    logger.success("All raw collections saved to Book Club ETL db.")
