"""Fetch, transform and store relevant MongoDB data for AuraDB upsertion."""

import json
import time
import datetime
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from pymongo.errors import PyMongoError
from src.utils.parsers import safe_value
from src.utils.security import decrypt_field
from src.utils.mongo_ops import load_sync_state, fetch_documents
from src.utils.connectors import connect_mongodb, close_mongodb, retry
from src.utils.polyglot import process_books, proceess_ur
from src.config import AURA_COLL_DIR


class Extract4AuraPipeline:
    """Pipeline for extracting and transforming MongoDB data for AuraDB."""

    SYNC_KEY = "auradb_sync"

    def __init__(self, workers: int = 8):
        self.db = connect_mongodb()
        self.etl_db = connect_mongodb("etl_metadata")
        self.timestamp = datetime.datetime.now()
        self.batch_id = time.strftime("%Y%m%d-%H%M%S")
        self.workers = workers
        self.last_sync_time = load_sync_state(self.etl_db, self.SYNC_KEY)
        self.output_dir = Path(AURA_COLL_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # Logging helpers
    def log_event(self, log_type, message, stats, context=None):
        """Insert a structured log entry into etl_metadata.logs."""
        try:
            log_doc = {
                "timestamp": time.time(),
                "pipeline": "auradb_extract",
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
                "pipeline": "auradb_extract",
                "type": "error",
                "batch_id": self.batch_id,
                "stage": stage,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "stack_trace": traceback.format_exc(),
                "context": extra_context or {},
            }
            self.etl_db["logs"].insert_one(log_doc)
        except PyMongoError as e:
            logger.error(f"Failed to write ETL error log: {e}")

    # Core helpers
    @retry(max_attempts=3, backoff=2)
    def _fetch_collection(self, name, collection, **kwargs):
        """Fetch a single collection with retry."""
        start = time.time()
        try:
            records = fetch_documents(collection, **kwargs)
            duration = int((time.time() - start) * 1000)
            stats = {
                "inserted": len(records),
                "updated": 0,
                "removed": 0,
                "duration_ms": duration,
                "retry_count": kwargs.get("_retry_count", 0),
            }
            self.log_event(
                "extract_collection",
                f"Fetched records for '{name}'",
                stats,
                context={"collection": name},
            )
            return name, records
        except Exception as exc:  # pylint: disable=W0718
            self.log_error("extract_collection", exc, extra_context={"collection": name})
            return name, []

    def extract(self):
        """Extract and transform all required collections from MongoDB."""
        start = time.time()
        lst = self.last_sync_time

        # Field maps
        books_map = {
            "author": "author.name",
            "author_id": "author._id",
            "series": "series.name",
            "series_id": "series._id",
        }

        bv_map = {
            "translator": "translator.name",
            "translator_id": "translator._id",
            "illustrator": "illustrator.name",
            "illustrator_id": "illustrator._id",
            "narrator": "narrator.name",
            "narrator_id": "narrator._id",
            "cover_artist": "cover_artist.name",
            "cover_artist_id": "cover_artist._id",
            "contributor": "contributor.name",
            "contributor_id": "contributor._id",
            "publisher_id": "publisher._id",
            "publisher": "publisher.name",
        }

        user_map = {
            "club_ids": "clubs._id",
            "badges": "badges.name",
            "badge_timestamps": "badges.timestamp",
        }

        club_map = {
            "badges": "badges.name",
            "badge_timestamps": "badges.timestamp",
        }

        excluded_user_fields = [
            "firstname",
            "lastname",
            "email_address",
            "password",
            "dob",
            "gender",
            "city",
            "state",
            "is_admin",
            "last_active_date",
        ]
        excluded_club_fields = ["member_permissions", "join_requests", "moderators"]

        # Collection configs
        tasks = {
            "books": {
                "collection": self.db["books"],
                "field_map": books_map,
                "since": lst,
            },
            "book_versions": {
                "collection": self.db["book_versions"],
                "field_map": bv_map,
                "since": lst,
            },
            "book_series": {
                "collection": self.db["book_series"],
                "exclude_fields": ["books"],
                "since": lst,
            },
            "genres": {
                "collection": self.db["genres"],
                "exclude_fields": ["date_added"],
                "since": lst,
            },
            "awards": {
                "collection": self.db["awards"],
                "exclude_fields": ["date_added"],
                "since": lst,
            },
            "creators": {
                "collection": self.db["creators"],
                "exclude_fields": ["date_added"],
                "since": lst,
            },
            "creator_roles": {
                "collection": self.db["creator_roles"],
                "since": lst,
            },
            "publishers": {
                "collection": self.db["publishers"],
                "exclude_fields": ["date_added"],
                "since": lst,
            },
            "formats": {
                "collection": self.db["formats"],
                "since": lst,
            },
            "languages": {
                "collection": self.db["languages"],
                "since": lst,
            },
            "user_badges": {
                "collection": self.db["user_badges"],
                "exclude_fields": ["date_added", "tiers"],
                "since": lst,
            },
            "club_badges": {
                "collection": self.db["club_badges"],
                "exclude_fields": ["date_added", "tiers"],
                "since": lst,
            },
            "countries": {
                "collection": self.db["countries"],
                "since": lst,
            },
            "users": {
                "collection": self.db["users"],
                "exclude_fields": excluded_user_fields,
                "field_map": user_map,
                "since": lst,
            },
            "clubs": {
                "collection": self.db["clubs"],
                "exclude_fields": excluded_club_fields,
                "field_map": club_map,
                "since": lst,
            },
            "user_reads": {
                "collection": self.db["user_reads"],
                "since": lst,
            },
            "club_period_books": {
                "collection": self.db["club_period_books"],
                "since": lst,
            },
            "club_reading_periods": {
                "collection": self.db["club_reading_periods"],
                "since": lst,
            }
        }

        results = {}

        # Concurrent fetch where safe
        if self.workers and self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(
                        self._fetch_collection,
                        name,
                        cfg["collection"],
                        exclude_fields=cfg.get("exclude_fields"),
                        field_map=cfg.get("field_map"),
                        since=cfg.get("since"),
                    ): name
                    for name, cfg in tasks.items()
                }

                for future in as_completed(futures):
                    name, records = future.result()
                    results[name] = records
        else:
            # Sequential fallback
            for name, cfg in tasks.items():
                _, records = self._fetch_collection(
                    name,
                    cfg["collection"],
                    exclude_fields=cfg.get("exclude_fields"),
                    field_map=cfg.get("field_map"),
                    since=cfg.get("since"),
                )
                results[name] = records

        # Enrich users
        current_year = datetime.date.today().year
        users = results.get("users", [])
        for user in users:
            goals = user.get("reading_goal", [])
            user["reading_goal"] = next(
                (g["goal"] for g in goals if g.get("year") == current_year),
                "N/A",
            )
            country = decrypt_field(user["country"], user["key_version"])
            user["country"] = country
            user.pop("key_version", None)

        # Enrich creators
        creators = results.get("creators", [])
        for creator in creators:
            firstname = creator.get("firstname", "")
            lastname = creator.get("lastname")
            creator["name"] = (
                f"{firstname} {lastname}".strip() if lastname else firstname
            )

        # Process books into books + book_awards
        books = results.get("books", [])
        books, book_awards = process_books(books)
        results["books"] = books
        results["book_awards"] = book_awards

        # Process user reads
        user_reads = results.get("user_reads", [])
        user_reads = proceess_ur(user_reads)
        results["user_reads"] = user_reads

        # Enrich club period books
        cpb = results.get("club_period_books", [])
        crp = results.get("club_reading_periods", [])
        for i in cpb:
            i["period_name"] = [k["name"] for k in crp if k["_id"] == i["period_id"]][0]
        results["club_period_books"] = cpb

        duration = int((time.time() - start) * 1000)
        stats = {
            "inserted": sum(len(v) for v in results.values()),
            "updated": 0,
            "removed": 0,
            "duration_ms": duration,
            "retry_count": 0,  # aggregate retry_count if you want later
        }
        self.log_event(
            "extract",
            "Extracted and transformed MongoDB data for AuraDB",
            stats,
        )

        return results

    def save_files(self, data):
        """Save extracted datasets to JSON files."""
        total_records = 0
        for name, records in data.items():
            records = safe_value(records)
            file_path = self.output_dir / f"{name}.json"
            start = time.time()
            try:
                logger.info(f"Saving {name} to {file_path}")
                with file_path.open("w", encoding="utf-8") as f:
                    json.dump(records, f, indent=2)

                duration = int((time.time() - start) * 1000)
                stats = {
                    "inserted": len(records),
                    "updated": 0,
                    "removed": 0,
                    "duration_ms": duration,
                    "retry_count": 0,
                }
                self.log_event(
                    "save_file",
                    f"Saved '{name}' to JSON",
                    stats,
                    context={"file": str(file_path)},
                )
                total_records += len(records)
            except Exception as exc:  # pylint: disable=W0718
                self.log_error(
                    "save_file",
                    exc,
                    extra_context={"file": str(file_path), "name": name},
                )

        return total_records

    def close(self):
        """Close MongoDB connections."""
        close_mongodb()

    def sync_all(self):
        """Run the full extraction pipeline."""
        batch_start = time.time()

        data = self.extract()
        total_records = self.save_files(data)

        duration_ms = int((time.time() - batch_start) * 1000)
        summary = {
            "timestamp": time.time(),
            "pipeline": "auradb_extract",
            "type": "batch_summary",
            "batch_id": self.batch_id,
            "total_files": len(data),
            "total_records": total_records,
            "total_retries": 0,  # can be extended to aggregate from _retry_count
            "duration_ms": duration_ms,
        }

        try:
            self.etl_db["batch_summaries"].insert_one(summary)
        except PyMongoError as e:
            logger.error(f"Failed to write extraction batch summary: {e}")

        logger.success("AuraDB extraction pipeline completed successfully.")
        return summary


# Run
if __name__ == "__main__":
    pipeline = Extract4AuraPipeline(workers=8)
    try:
        pipeline.sync_all()
    finally:
        pipeline.close()
