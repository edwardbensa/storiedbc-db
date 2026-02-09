"""Transform new staging data for upload to main database."""

# Imports
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from loguru import logger
from src.config import STAGING_COLL_DIR, MAIN_COLL_DIR
from src.utils.lookups import load_lookup_data, resolve_lookup, resolve_creator, resolve_awards
from src.utils.mongo_ops import download_collections, load_sync_state, fetch_documents
from src.utils.connectors import connect_azure_blob, connect_mongodb, close_mongodb
from src.utils.security import encrypt_pii, hash_password, latest_key_version
from src.utils.parsers import to_int, to_float, to_array, make_subdocuments
from src.utils.transforms import (transform_collection, add_read_details,
                                  set_custom_ids, remove_custom_ids)
from src.utils.fields import generate_image_url


# pylint: disable=W0621
# pylint: disable=W0613

# Transform functions
def transform_books(doc, *, context):
    """
    Transforms a books document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "book_id": doc.get("book_id"),
        "title": doc.get("title"),
        "author": make_subdocuments(doc.get("author"), "creators", subdoc_registry, separator=','),
        "genre": to_array(doc.get("genre")),
        "series": resolve_lookup('book_series', doc.get("series"), lookup_data),
        "series_index": to_int(doc.get("series_index")),
        "description": doc.get("description"),
        "first_publication_date": doc.get("first_publication_date"),
        "contributors": make_subdocuments(doc.get("contributors"), "creators", subdoc_registry,','),
        "awards": make_subdocuments(doc.get("awards"), "awards", subdoc_registry, separator='|'),
        "tags": to_array(doc.get("tags")),
        "date_added": doc.get("date_added")
    }

def transform_book_versions(doc, *, context):
    """
    Transforms a book_versions document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    subdoc_registry = context["subdoc_registry"]
    blob_acc = context["blob_account"]

    return {
        "_id": doc.get("_id"),
        "book_id": resolve_lookup('books', doc.get("book_id"), lookup_data),
        "title": doc.get("title"),
        "isbn_13": to_int(doc.get("isbn_13")),
        "asin": doc.get("asin"),
        "format": doc.get("format"),
        "edition": doc.get("edition"),
        "release_date": doc.get("release_date"),
        "page_count": to_int(doc.get("page_count")),
        "length_hours": to_float(doc.get("length")),
        "description": doc.get("description"),
        "publisher": resolve_lookup('publishers', doc.get("publisher"), lookup_data),
        "language": doc.get("language"),
        "translator": make_subdocuments(doc.get("translator"), "creators", subdoc_registry, ','),
        "narrator": make_subdocuments(doc.get("narrator"), "creators", subdoc_registry, ','),
        "illustrator": make_subdocuments(doc.get("illustrator"), "creators", subdoc_registry, ','),
        "editors": make_subdocuments(doc.get("editors"), "creators", subdoc_registry, ','),
        "cover_artist": make_subdocuments(doc.get("cover_artist"), "creators", subdoc_registry,','),
        "cover_url": generate_image_url(doc, doc.get("cover_url"), "cover", "cover-art", blob_acc),
        "date_added": doc.get("date_added")
    }

def transform_book_series(doc, *, context):
    """
    Transforms a book_series document to the desired structure.
    """
    books = context["books"]
    filtered_books = [b for b in books if b["series"] == doc.get("name")]
    filtered_books.sort(key=lambda b: b["series_index"])
    selected = [{"index": to_int(b["series_index"]), "_id": b["_id"]} for b in filtered_books]

    return{
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "books": selected,
        "date_added": doc.get("date_added")
    }

def transform_club_members(doc, *, context):
    """
    Transforms a club_members document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "user_id": lookup_data["users"].get(doc.get("user_id")),
        "role": doc.get("role"),
        "date_joined": doc.get("date_joined"),
        "is_active": doc.get("is_active") == "TRUE",
    }

def transform_club_member_reads(doc, *, context):
    """
    Transforms a club_member_reads document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "user_id": lookup_data["users"].get(doc.get("user_id")),
        "book_id": lookup_data["books"].get(doc.get("book_id")),
        "period_id": lookup_data["club_reading_periods"].get(doc.get("period_id")),
        "read_date": doc.get("read_date"),
        "timestamp": str(datetime.now())
    }

def transform_club_period_books(doc, *, context):
    """
    Transforms a club_period_books document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "book_id": lookup_data["books"].get(doc.get("book_id")),
        "period_id": lookup_data["club_reading_periods"].get(doc.get("period_id")),
        "period_startdate": doc.get("period_startdate"),
        "period_enddate": doc.get("period_enddate"),
        "selected_by": lookup_data["users"].get(doc.get("selected_by")),
        "selection_method": doc.get("selection_method"),
        "votes": make_subdocuments(doc.get("votes"), "votes", subdoc_registry, separator=";"),
        "votes_startdate": doc.get("votes_startdate"),
        "votes_enddate": doc.get("votes_enddate"),
        "selection_status": doc.get("selection_status"),
        "date_added": str(datetime.now())
    }

def transform_club_discussions(doc, *, context):
    """
    Transforms a club_discussions document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "topic_name": doc.get("topic_name"),
        "topic_description": doc.get("topic_description"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "timestamp": doc.get("timestamp"),
        "comments": make_subdocuments(
            doc.get("comments"), "club_discussions", subdoc_registry, separator="|"),
        "book_reference": lookup_data["books"].get(doc.get("book_reference"))
    }

def transform_club_events(doc, *, context):
    """
    Transforms a club_events document to the desired structure.
    """
    lookup_data = context["lookup_data"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "type": doc.get("type"),
        "startdate": doc.get("startdate"),
        "enddate": doc.get("enddate"),
        "status": doc.get("status"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "date_added": str(datetime.now())
    }

def transform_club_reading_periods(doc, *, context):
    """
    Transforms a club_reading_periods document to the desired structure.
    """
    lookup_data = context["lookup_data"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "startdate": doc.get("startdate"),
        "enddate": doc.get("enddate"),
        "status": doc.get("period_status"),
        "max_books": to_int(doc.get("max_books")),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "date_added": str(datetime.now())
    }

def transform_club_badges(doc, *, context):
    """
    Transforms a club_badges document to the desired structure.
    """
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "category": doc.get("category"),
        "tiers": make_subdocuments(
            doc.get("tiers"), "tiers", subdoc_registry, separator="|"),
        "description": doc.get("description"),
        "date_added": str(datetime.now())
    }

def transform_clubs(doc, *, context):
    """
    Transforms a clubs document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "handle": doc.get("handle"),
        "name": doc.get("name"),
        "creationdate": doc.get("creationdate"),
        "preferred_genres": to_array(doc.get("preferred_genres")),
        "description": doc.get("description"),
        "visibility": doc.get("visibility"),
        "rules": doc.get("rules"),
        "moderators": [lookup_data["users"].get(user) for user
                            in to_array(doc.get("moderators"))],
        "badges": make_subdocuments(doc.get("badges"), "club_badges",
                                    subdoc_registry, separator='|'),
        "member_permissions": to_array(doc.get("member_permissions")),
        "join_requests": make_subdocuments(doc.get("join_requests"), "join_requests",
                                           subdoc_registry, separator=";"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
    }

def transform_user_reads(doc, *, context):
    """
    Transforms a user_reads document to the desired structure.
    """
    lookup_data = context["lookup_data"]
    subdoc_registry = context["subdoc_registry"]

    # Modify doc
    book_versions = context["book_versions"]
    doc = add_read_details(doc, book_versions)

    return {
        "_id": doc.get("_id"),
        "user_id": resolve_lookup('users', doc.get("user_id"), lookup_data),
        "version_id": resolve_lookup('book_versions', doc.get("version_id"), lookup_data),
        "rstatus": resolve_lookup('read_statuses', doc.get("rstatus_id"), lookup_data),
        "reading_log": make_subdocuments(doc.get("reading_log"), 'reading_log',
                                             subdoc_registry, separator=','),
        "date_started": doc.get("date_started"),
        "date_completed": doc.get("date_completed"),
        "days_to_read": doc.get("days_to_read"),
        "pages_per_day": doc.get("pages_per_day"),
        "hours_per_day": doc.get("hours_per_day"),
        "rating": None if doc.get("rating") == "" else int(doc.get("rating")),
        "notes": doc.get("notes"),
    }

def transform_user_roles(doc, *, context):
    """
    Transforms a user_roles document to the desired structure.
    """
    return {
        "_id": doc.get("role_id"),
        "name": doc.get("name"),
        "permissions": to_array(doc.get("permissions")),
        "description": doc.get("description"),
        "date_added": str(datetime.now())
    }

def transform_user_badges(doc, *, context):
    """
    Transforms a user_badges document to the desired structure.
    """
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "category": doc.get("category"),
        "tiers": make_subdocuments(
            doc.get("tiers"), "tiers", subdoc_registry, separator="|"),
        "description": doc.get("description"),
        "date_added": str(datetime.now())
    }

def transform_users(doc, *, context):
    """
    Transforms a user document to the desired structure.
    """
    subdoc_registry = context["subdoc_registry"]
    key_version = latest_key_version

    return {
        "_id": doc.get("_id"),
        "user_id": doc.get("user_id"),
        "handle": doc.get("handle"),
        "firstname": doc.get("firstname"),
        "lastname": doc.get("lastname"),
        "email_address": encrypt_pii(doc.get("email_address"), version=key_version),
        "password": hash_password(doc.get("password")),
        "dob": encrypt_pii(doc.get("dob"), version=key_version),
        "gender": encrypt_pii(doc.get("gender"), version=key_version),
        "city": encrypt_pii(doc.get("city"), version=key_version),
        "state": encrypt_pii(doc.get("state"), version=key_version),
        "country": encrypt_pii(doc.get("country"), version=key_version),
        "bio": doc.get("bio"),
        "reading_goal": make_subdocuments(doc.get("reading_goal"), 'reading_goal',
                                             subdoc_registry, separator='|'),
        "badges": make_subdocuments(doc.get("badges"), "user_badges",
                                             subdoc_registry, separator='|'),
        "preferred_genres": to_array(doc.get("preferred_genres")),
        "forbidden_genres": to_array(doc.get("forbidden_genres")),
        "clubs": make_subdocuments(doc.get("clubs"), 'clubs',
                                        subdoc_registry, separator='|'),
        "date_joined": doc.get("date_joined"),
        "last_active_date": doc.get("last_active_date"),
        "is_admin": bool(doc.get("is_admin", False)),
        "key_version": key_version
    }

def transform_creators(doc, *, context):
    """
    Transforms a creators document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "creator_id": doc.get("creator_id"),
        "firstname": doc.get("firstname"),
        "lastname": doc.get("lastname"),
        "bio": doc.get("bio"),
        "website": doc.get("website"),
        "roles": to_array(doc.get("roles")),
        "date_added": str(datetime.now())
    }

def transform_awards(doc, *, context):
    """
    Transforms an awards document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "org": doc.get("org"),
        "description": doc.get("description"),
        "website": doc.get("website"),
        "categories": to_array(doc.get("categories")),
        "statuses": to_array(doc.get("statuses")),
        "year_started": to_int(doc.get("year_started")),
        "year_ended": to_int(doc.get("year_ended"))
    }

raw_collections_to_cleanup = {
    "genres": "genre_id",
    "publishers": "publisher_id",
    "tags": "tag_id",
}

# Collections to modify id_fields to use custom string _ids
collections_to_modify = {
    "formats": "format_id",
    "languages": "language_id",
    "creator_roles": "cr_id",
    "read_statuses": "rstatus_id",
    "club_event_types": "event_type_id",
    "club_event_statuses": "event_status_id",
    "user_permissions": "permission_id",
    "countries": "country_id"
}

transform_map = {
    "club_members": transform_club_members,
    "club_member_reads": transform_club_member_reads,
    "club_period_books": transform_club_period_books,
    "club_discussions": transform_club_discussions,
    "club_events": transform_club_events,
    "club_reading_periods": transform_club_reading_periods,
    "club_badges": transform_club_badges,
    "clubs": transform_clubs,
    "user_reads": transform_user_reads,
    "user_roles": transform_user_roles,
    "user_badges": transform_user_badges,
    "users": transform_users,
    "books": transform_books,
    "book_versions": transform_book_versions,
    "book_series": transform_book_series,
    "creators": transform_creators,
    "awards": transform_awards,
}

if __name__ == "__main__":
    # Connect to db
    staging_db = connect_mongodb("staging")
    etl_db = connect_mongodb("etl_metadata")

    # Load sync state
    lst = load_sync_state(etl_db, "main_sync")

    # Download deltas and capture count
    logger.info("Fetching collections...")
    DELTA_COUNT = download_collections(staging_db, STAGING_COLL_DIR, ["deletions"], lst)

    # Early exit signal
    if DELTA_COUNT == 0:
        logger.info("No new data found in staging. Signalling early pipeline stop.")
        # Exit code 10 = "Success, but nothing to process"
        sys.exit(10)

    # Blob Service Client
    blob_acc = connect_azure_blob().account_name

    # Create directories
    logger.info("Creating bronze and silver collection folders..")
    os.makedirs(STAGING_COLL_DIR, exist_ok=True)
    os.makedirs(MAIN_COLL_DIR, exist_ok=True)

    # Load files
    books = fetch_documents(collection=staging_db["books"],
            query={"series": {"$ne": ""}}, flatten=False)
    user_reads_updates = fetch_documents(staging_db["user_reads"], since=lst)
    version_ids = {doc["version_id"] for doc in user_reads_updates}
    book_versions = fetch_documents(collection=staging_db["book_versions"],
            query={"version_id": {"$in": list(version_ids)}}, flatten=False)

    # Load and map all lookup collections
    lookup_registry = {
        "creators": {"field": 'creator_id', "get": ["_id", "firstname", "lastname"]},
        "book_series": {"field": "name", "get": ["_id", "name"]},
        "awards": {"field": 'award_id', "get": "_id"},
        "publishers": {"field": "name", "get": ["_id", "name"]},
        "books": {"field": "book_id", "get": "_id"},
        "users": {"field": "user_id", "get": "_id"},
        "clubs": {"field": "club_id", "get": "_id"},
        "club_reading_periods": {"field": "period_id", "get": "_id"},
        "genres": {"field": "genre_name", "get": ["_id", "name"]},
        "club_badges": {"field": "name", "get": ["_id", "name"]},
        "user_badges": {"field": "name", "get": ["_id", "name"]},
        "book_versions": {"field": 'version_id', "get": "_id"},
        "read_statuses": {"field": 'rstatus_id', "get": "name"},
    }

    logger.info("Building in-memory lookup maps from staging database...")
    lookup_data = load_lookup_data(staging_db, lookup_registry)

    # Close database
    close_mongodb()
    logger.success(f"Lookup data loaded and collections successfully saved to {STAGING_COLL_DIR}")

    # Create subdocument registry
    subdoc_registry= {
        "creators": {
            "pattern": None,
            "transform": lambda name: resolve_creator(name.strip(), lookup_data)
        },
        "awards": {
            "pattern": re.compile(
                r"award_id:\s*(\w+);\s*"
                r"award_name:\s*(.*?);\s*"
                r"award_category:\s*(.*?);\s*"
                r"year:\s*(\d{4});\s*"
                r"award_status:\s*(\w+)"
            ),
            "transform": lambda match: resolve_awards(match, lookup_data)
        },
        "votes": {
            "pattern": re.compile(r"user_id:\s*(\w+),\s*vote_date:\s*(\d{4}-\d{2}-\d{2})"),
            "transform": lambda match: {
                "user_id": lookup_data["users"].get(match.group(1)),
                "timestamp": match.group(2)
            }
        },
        "club_discussions": {
            "pattern": re.compile(
                r"user_id:\s*(\w+);\s*comment:\s*(.+?);\s*timestamp:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})" # pylint: disable=C0301
            ),
            "transform": lambda match: {
                "user_id": lookup_data["users"].get(match.group(1)),
                "comment": match.group(2).strip(),
                "timestamp": match.group(3)
            }
        },
        "club_genres": {
            "pattern": None,
            "transform": lambda genre_name: lookup_data["genres"].get(genre_name.strip())
        },
        "join_requests": {
            "pattern": re.compile(r"user_id:\s*(\w+),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})"),
            "transform": lambda match: {
                "user_id": lookup_data["users"].get(match.group(1)),
                "timestamp": match.group(2)
            }
        },
        "club_badges": {
            "pattern": re.compile(r"badge:\s*(.+?),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})"),
            "transform": lambda match: {
                **resolve_lookup("club_badges", match.group(1), lookup_data), # type: ignore
                "timestamp": match.group(2)
            }
        },
        'reading_log': {
            'pattern': re.compile(r'(.+):\s*(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?)'),
            'transform': lambda match: {
                "rstatus": resolve_lookup('read_statuses', match.group(1), lookup_data),
                "timestamp": match.group(2)
            }
        },
        'reading_goal': {
            'pattern': re.compile(r'year:\s*(\d+),\s*goal:\s*(\d+)'),
            'transform': lambda match: {
                "year": to_int(match.group(1)),
                "goal": to_int(match.group(2))
            }
        },
        'user_badges': {
            'pattern': re.compile(r'badge:\s*(.+?),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})'),
            'transform': lambda match: {
                **resolve_lookup('user_badges', match.group(1), lookup_data), # type: ignore
                "timestamp": match.group(2)
            }
        },
        'preferred_genres': {
            'pattern': None,
            'transform': lambda genre_name: resolve_lookup('genres', genre_name, lookup_data)
        },
        "clubs": {
            "pattern": re.compile(
                r"_id:\s*(\w+),\s*role:\s*(\w+),\s*joined:\s*(\d{4}-\d{2}-\d{2})"
            ),
            "transform": lambda match: {
                "_id": lookup_data["clubs"].get(match.group(1)),
                "role": match.group(2)
            }
        },
        "tiers": {
            "pattern": re.compile(r"name:\s*(.*?),\s*threshold:\s*(\d+),\s*message:\s*(.*)"),
            "transform": lambda match: {
                "name": match.group(1).strip(),
                "threshold": int(match.group(2)),
                "message": match.group(3).strip()
            }
        }
    }

    # Create context
    context = {
        "lookup_data": lookup_data,
        "subdoc_registry": subdoc_registry,
        "books": books,
        "book_versions": book_versions,
        "blob_account": blob_acc,
    }

    delta_files = [f.stem for f in Path(STAGING_COLL_DIR).glob("*.json")]

    if not delta_files:
        logger.info("No new deltas found. Transformation skipped.")
    else:
        logger.info(f"Dynamically processing {len(delta_files)} collections...")
        for collection in delta_files:
            if collection in transform_map:
                logger.info(f"Transforming: {collection}")
                transform_collection(collection, transform_map[collection], context=context)
            else:
                logger.warning(f"No transformation function mapped for '{collection}'. Skipping.")

    remove_custom_ids(raw_collections_to_cleanup, STAGING_COLL_DIR)
    set_custom_ids(collections_to_modify)
    logger.info("Cleaned collections.")
