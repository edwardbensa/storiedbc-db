"""Staging DB -> main DB data transformation utility functions."""

# Imports
import os
import re
import json
from datetime import datetime, timezone
from loguru import logger
from src.config import STAGING_COLL_DIR, MAIN_COLL_DIR
from .security import encrypt_pii, hash_password, latest_key_version
from .lookups import resolve_lookup, resolve_creator, resolve_awards, load_lookup_maps
from .fields import generate_image_url, generate_rlog, compute_d2r, compute_rr, find_doc
from .parsers import to_int, to_float, to_array, make_subdocuments, clean_document, safe_value


# pylint: disable=W0613


# LOOKUP DATA AND SUBDOCUMENT REGISTRY

def build_registries(db):
    """
    Returns lookup data for _id reference
    and subdocument registry for parsing flat fields.
    """

    lookup_registry = {
        "creators": {"field": "creator_id", "get": ["_id", "firstname", "lastname"]},
        "book_series": {"field": "name", "get": ["_id", "name"]},
        "awards": {"field": 'award_id', "get": "_id"},
        "publishers": {"field": "name", "get": ["_id", "name"]},
        "books": {"field": "book_id", "get": "_id"},
        "users": {"field": "user_id", "get": "_id"},
        "clubs": {"field": "club_id", "get": "_id"},
        "club_reading_periods": {"field": "period_id", "get": ["_id", "name"]},
        "genres": {"field": "name", "get": ["_id", "name"]},
        "club_badges": {"field": "name", "get": ["_id", "name"]},
        "user_badges": {"field": "name", "get": ["_id", "name"]},
        "book_versions": {"field": 'version_id', "get": "_id"},
        "read_statuses": {"field": 'rstatus_id', "get": "name"},
    }

    logger.info("Building in-memory lookup maps from staging database...")
    lookup_map = load_lookup_maps(db, lookup_registry)

    subdocument_registry = {
        "creators": {
            "pattern": None,
            "transform": lambda name: resolve_creator(name.strip(), lookup_map)
        },
        "awards": {
            "pattern": re.compile(
                r"award_id:\s*(\w+);\s*"
                r"award_name:\s*(.*?);\s*"
                r"award_category:\s*(.*?);\s*"
                r"year:\s*(\d{4});\s*"
                r"award_status:\s*(\w+)"
            ),
            "transform": lambda match: resolve_awards(match, lookup_map)
        },
        "votes": {
            "pattern": re.compile(r"user_id:\s*(\w+),\s*vote_date:\s*(\d{4}-\d{2}-\d{2})"),
            "transform": lambda match: {
                "user_id": lookup_map["users"].get(match.group(1)),
                "timestamp": match.group(2)
            }
        },
        "club_discussions": {
            "pattern": re.compile(
                r"user_id:\s*(\w+);\s*comment:\s*(.+?);\s*timestamp:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})" # pylint: disable=C0301
            ),
            "transform": lambda match: {
                "user_id": lookup_map["users"].get(match.group(1)),
                "comment": match.group(2).strip(),
                "timestamp": match.group(3)
            }
        },
        "club_genres": {
            "pattern": None,
            "transform": lambda genre_name: lookup_map["genres"].get(genre_name.strip())
        },
        "join_requests": {
            "pattern": re.compile(r"user_id:\s*(\w+),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})"),
            "transform": lambda match: {
                "user_id": lookup_map["users"].get(match.group(1)),
                "timestamp": match.group(2)
            }
        },
        "club_badges": {
            "pattern": re.compile(r"badge:\s*(.+?),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})"),
            "transform": lambda match: {
                **resolve_lookup("club_badges", match.group(1), lookup_map), # type: ignore
                "timestamp": match.group(2)
            }
        },
        'reading_log': {
            'pattern': re.compile(r'(.+):\s*(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?)'),
            'transform': lambda match: {
                "rstatus": resolve_lookup('read_statuses', match.group(1), lookup_map),
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
                **resolve_lookup('user_badges', match.group(1), lookup_map), # type: ignore
                "timestamp": match.group(2)
            }
        },
        'preferred_genres': {
            'pattern': None,
            'transform': lambda genre_name: resolve_lookup('genres', genre_name, lookup_map)
        },
        "clubs": {
            "pattern": re.compile(
                r"_id:\s*(\w+),\s*role:\s*(\w+),\s*joined:\s*(\d{4}-\d{2}-\d{2})"
            ),
            "transform": lambda match: {
                "_id": lookup_map["clubs"].get(match.group(1)),
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

    return lookup_map, subdocument_registry


# GENERAL FUNCTIONS

def transform_collection(collection_name: str, transform_func, *, context=None):
    """
    Loads a raw JSON collection, transforms each document,
    and writes the result to MAIN_COLL_DIR.
    Assumes _id is already present in the input.
    """
    input_path = os.path.join(STAGING_COLL_DIR, f"{collection_name}.json")
    output_path = os.path.join(MAIN_COLL_DIR, f"{collection_name}.json")

    try:
        with open(input_path, encoding="utf-8") as f:
            raw_docs = json.load(f)

        transformed = []
        removed_keys = []
        for doc in raw_docs:
            clean_doc, removed = clean_document(transform_func(doc, context=context))
            clean_doc = safe_value(clean_doc)
            transformed.append(clean_doc)
            removed_keys.extend(removed)

        counts = {item: removed_keys.count(item) for item in sorted(set(removed_keys))}
        if counts != {}:
            logger.warning(f"The following keys were removed: {counts}")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=2)

        logger.info(f"Transformed {len(transformed)} records -> {output_path}")

    except FileNotFoundError:
        logger.warning(f"Raw JSON file not found: {input_path}")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Error transforming '{collection_name}': {e}")


def add_read_details(doc, book_versions):
    """Add reading log, days to read, and read rates."""

    # Skip if current_rstatus is "To Read"
    current_rstatus = doc["rstatus_id"]
    if current_rstatus == "rs4":
        doc["reading_log"] = ""
        doc.pop("rstatus_history")
        return doc

    version_id = doc.get("version_id")
    version_doc = find_doc(book_versions, "version_id", version_id)

    # Add reading log and days to read
    doc["reading_log"] = generate_rlog(doc)
    doc["days_to_read"] = compute_d2r(doc)

    # Add read rate
    metric = "hours" if version_doc["format"] == "audiobook" else "pages"
    doc[f"{metric}_per_day"] = compute_rr(doc, book_versions)

    doc.pop("rstatus_history")
    return doc


# COLLECTION SPECIFIC FUNCTIONS

def transform_books(doc, *, context):
    """Transforms a books document to the desired structure."""
    lookups = context["lookup_map"]
    subdoc_reg = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "title": str(doc.get("title")),
        "author": make_subdocuments(doc.get("author"), "creators", subdoc_reg, ','),
        "genre": to_array(doc.get("genre")),
        "series": resolve_lookup('book_series', doc.get("series"), lookups),
        "series_index": to_int(doc.get("series_index")),
        "description": doc.get("description"),
        "first_publication_date": doc.get("first_publication_date"),
        "contributors": make_subdocuments(doc.get("contributors"), "creators", subdoc_reg, ','),
        "awards": make_subdocuments(doc.get("awards"), "awards", subdoc_reg, '|'),
        "tags": to_array(doc.get("tags")),
        "date_added": doc.get("date_added")
    }

def transform_book_versions(doc, *, context):
    """Transforms a book_versions document to the desired structure."""
    lookup_map = context["lookup_map"]
    subdoc_reg = context["subdoc_registry"]
    blob_acc = context["blob_account"]

    return {
        "_id": doc.get("_id"),
        "book_id": resolve_lookup('books', doc.get("book_id"), lookup_map),
        "title": doc.get("title"),
        "isbn_13": to_int(doc.get("isbn_13")),
        "asin": doc.get("asin"),
        "format": doc.get("format"),
        "edition": doc.get("edition"),
        "release_date": doc.get("release_date"),
        "page_count": to_int(doc.get("page_count")),
        "length_hours": to_float(doc.get("length")),
        "description": doc.get("description"),
        "publisher": resolve_lookup('publishers', doc.get("publisher"), lookup_map),
        "language": doc.get("language"),
        "translator": make_subdocuments(doc.get("translator"), "creators", subdoc_reg, ','),
        "narrator": make_subdocuments(doc.get("narrator"), "creators", subdoc_reg, ','),
        "illustrator": make_subdocuments(doc.get("illustrator"), "creators", subdoc_reg, ','),
        "editors": make_subdocuments(doc.get("editors"), "creators", subdoc_reg, ','),
        "cover_artist": make_subdocuments(doc.get("cover_artist"), "creators", subdoc_reg,','),
        "cover_url": generate_image_url(doc, doc.get("cover_url"), "cover", "cover-art", blob_acc),
        "date_added": doc.get("date_added")
    }

def transform_book_series(doc, *, context):
    """Transforms a book_series document to the desired structure."""
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
    """Transforms a club_members document to the desired structure."""
    lookup_map = context["lookup_map"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_map["clubs"].get(doc.get("club_id")),
        "user_id": lookup_map["users"].get(doc.get("user_id")),
        "role": doc.get("role"),
        "date_joined": doc.get("date_joined"),
        "is_active": doc.get("is_active") == "TRUE",
    }

def transform_club_member_reads(doc, *, context):
    """Transforms a club_member_reads document to the desired structure."""
    lookup_map = context["lookup_map"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_map["clubs"].get(doc.get("club_id")),
        "user_id": lookup_map["users"].get(doc.get("user_id")),
        "book_id": lookup_map["books"].get(doc.get("book_id")),
        "period_id": lookup_map["club_reading_periods"].get(doc.get("period_id"))["_id"],
        "read_date": doc.get("read_date"),
        "timestamp": str(datetime.now(timezone.utc))
    }

def transform_club_period_books(doc, *, context):
    """Transforms a club_period_books document to the desired structure."""
    lookup_map = context["lookup_map"]
    subdoc_reg = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_map["clubs"].get(doc.get("club_id")),
        "book_id": lookup_map["books"].get(doc.get("book_id")),
        "period": lookup_map["club_reading_periods"].get(doc.get("period_id")),
        "period_startdate": doc.get("period_startdate"),
        "period_enddate": doc.get("period_enddate"),
        "selected_by": lookup_map["users"].get(doc.get("selected_by")),
        "selection_method": doc.get("selection_method"),
        "votes": make_subdocuments(doc.get("votes"), "votes", subdoc_reg, ";"),
        "votes_startdate": doc.get("votes_startdate"),
        "votes_enddate": doc.get("votes_enddate"),
        "selection_status": doc.get("selection_status"),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_club_discussions(doc, *, context):
    """Transforms a club_discussions document to the desired structure."""
    lookup_map = context["lookup_map"]
    subdoc_reg = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_map["clubs"].get(doc.get("club_id")),
        "topic_name": doc.get("topic_name"),
        "topic_description": doc.get("topic_description"),
        "created_by": lookup_map["users"].get(doc.get("created_by")),
        "timestamp": doc.get("timestamp"),
        "comments": make_subdocuments(doc.get("comments"), "club_discussions", subdoc_reg, "|"),
        "book_reference": lookup_map["books"].get(doc.get("book_reference"))
    }

def transform_club_events(doc, *, context):
    """Transforms a club_events document to the desired structure."""
    lookup_map = context["lookup_map"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_map["clubs"].get(doc.get("club_id")),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "type": doc.get("type"),
        "startdate": doc.get("startdate"),
        "enddate": doc.get("enddate"),
        "status": doc.get("status"),
        "created_by": lookup_map["users"].get(doc.get("created_by")),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_club_reading_periods(doc, *, context):
    """Transforms a club_reading_periods document to the desired structure."""
    lookup_map = context["lookup_map"]

    return {
        "_id": doc.get("_id"),
        "club_id": lookup_map["clubs"].get(doc.get("club_id")),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "startdate": doc.get("startdate"),
        "enddate": doc.get("enddate"),
        "status": doc.get("period_status"),
        "max_books": to_int(doc.get("max_books")),
        "created_by": lookup_map["users"].get(doc.get("created_by")),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_club_badges(doc, *, context):
    """Transforms a club_badges document to the desired structure."""
    subdoc_reg = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "category": doc.get("category"),
        "tiers": make_subdocuments(doc.get("tiers"), "tiers", subdoc_reg, "|"),
        "description": doc.get("description"),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_club_event_types(doc, *, context=None):
    """Transforms a club_event_types document to the desired structure."""
    return {
        "_id": doc.get("et_id"),
        "name": doc.get("name"),
        "category": doc.get("category"),
        "description": doc.get("description"),
    }

def transform_club_event_statuses(doc, *, context=None):
    """Transforms a club_event_statuses document to the desired structure."""
    return {
        "_id": doc.get("es_id"),
        "name": doc.get("name"),
        "description": doc.get("description"),
    }

def transform_clubs(doc, *, context):
    """Transforms a clubs document to the desired structure."""
    lookup_map = context["lookup_map"]
    subdoc_reg = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "handle": doc.get("handle"),
        "name": doc.get("name"),
        "creationdate": doc.get("creationdate"),
        "preferred_genres": to_array(doc.get("preferred_genres")),
        "description": doc.get("description"),
        "visibility": doc.get("visibility"),
        "rules": doc.get("rules"),
        "moderators": [lookup_map["users"].get(user) for user in to_array(doc.get("moderators"))],
        "badges": make_subdocuments(doc.get("badges"), "club_badges", subdoc_reg, '|'),
        "member_permissions": to_array(doc.get("member_permissions")),
        "join_requests": make_subdocuments(doc.get("join_requests"),"join_requests",subdoc_reg,";"),
        "created_by": lookup_map["users"].get(doc.get("created_by")),
    }

def transform_user_reads(doc, *, context):
    """Transforms a user_reads document to the desired structure."""
    lookup_map = context["lookup_map"]
    subdoc_reg = context["subdoc_registry"]

    # Modify doc
    book_versions = context["book_versions"]
    doc = add_read_details(doc, book_versions)

    return {
        "_id": doc.get("_id"),
        "user_id": resolve_lookup('users', doc.get("user_id"), lookup_map),
        "version_id": resolve_lookup('book_versions', doc.get("version_id"), lookup_map),
        "rstatus": resolve_lookup('read_statuses', doc.get("rstatus_id"), lookup_map),
        "reading_log": make_subdocuments(doc.get("reading_log"), 'reading_log', subdoc_reg, ','),
        "date_started": doc.get("date_started"),
        "date_completed": doc.get("date_completed"),
        "days_to_read": doc.get("days_to_read"),
        "pages_per_day": doc.get("pages_per_day"),
        "hours_per_day": doc.get("hours_per_day"),
        "rating": None if doc.get("rating") == "" else int(doc.get("rating")),
        "notes": doc.get("notes"),
    }

def transform_user_roles(doc, *, context):
    """Transforms a user_roles document to the desired structure."""
    return {
        "_id": doc.get("role_id"),
        "name": doc.get("name"),
        "permissions": to_array(doc.get("permissions")),
        "description": doc.get("description"),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_user_badges(doc, *, context):
    """Transforms a user_badges document to the desired structure."""
    subdoc_registry = context["subdoc_registry"]

    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "category": doc.get("category"),
        "tiers": make_subdocuments( doc.get("tiers"), "tiers", subdoc_registry, "|"),
        "description": doc.get("description"),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_user_permissions(doc, *, context):
    """Transforms a user_permissions document to the desired structure."""
    return {
        "_id": doc.get("permission_id"),
        "name": doc.get("name"),
        "description": doc.get("description"),
    }

def transform_users(doc, *, context):
    """Transforms a user document to the desired structure."""
    subdoc_reg = context["subdoc_registry"]
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
        "reading_goal": make_subdocuments(doc.get("reading_goal"), 'reading_goal', subdoc_reg, '|'),
        "badges": make_subdocuments(doc.get("badges"), "user_badges", subdoc_reg, '|'),
        "preferred_genres": to_array(doc.get("preferred_genres")),
        "forbidden_genres": to_array(doc.get("forbidden_genres")),
        "clubs": make_subdocuments(doc.get("clubs"), 'clubs', subdoc_reg, '|'),
        "date_joined": doc.get("date_joined"),
        "last_active_date": doc.get("last_active_date"),
        "is_admin": bool(doc.get("is_admin", False)),
        "key_version": key_version
    }

def transform_creators(doc, *, context):
    """Transforms a creators document to the desired structure."""
    return {
        "_id": doc.get("_id"),
        "creator_id": doc.get("creator_id"),
        "firstname": doc.get("firstname"),
        "lastname": doc.get("lastname"),
        "bio": doc.get("bio"),
        "website": doc.get("website"),
        "roles": to_array(doc.get("roles")),
        "date_added": str(datetime.now(timezone.utc))
    }

def transform_awards(doc, *, context):
    """Transforms an awards document to the desired structure."""
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

def transform_tags(doc, *, context):
    """Transforms a tags document to the desired structure."""
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
    }

def transform_genres(doc, *, context):
    """Transforms a genres document to the desired structure."""
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
    }

def transform_publishers(doc, *, context):
    """Transforms a publishers document to the desired structure."""
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "url": doc.get("url"),
    }

def transform_formats(doc, *, context):
    """Transforms a formats document to the desired structure."""
    return {
        "_id": doc.get("format_id"),
        "name": doc.get("name"),
    }

def transform_languages(doc, *, context):
    """Transforms a languages document to the desired structure."""
    return {
        "_id": doc.get("language_id"),
        "name": doc.get("name"),
        "code":	doc.get("code"),
        "class": doc.get("class"),
    }

def transform_creator_roles(doc, *, context):
    """Transforms a creator_roles document to the desired structure."""
    return {
        "_id": doc.get("cr_id"),
        "name": doc.get("name"),
    }

def transform_read_statuses(doc, *, context):
    """Transforms a read_statuses document to the desired structure."""
    return {
        "_id": doc.get("rstatus_id"),
        "name": doc.get("name"),
    }

def transform_countries(doc, *, context):
    """Transforms a countries document to the desired structure."""
    return {
        "_id": doc.get("country_id"),
        "name": doc.get("name"),
        "continent": doc.get("continent"),
    }


# TRANSFORM MAP
transform_map = {
    "club_members": transform_club_members,
    "club_member_reads": transform_club_member_reads,
    "club_period_books": transform_club_period_books,
    "club_discussions": transform_club_discussions,
    "club_events": transform_club_events,
    "club_event_types": transform_club_event_types,
    "club_event_statuses": transform_club_event_statuses,
    "club_reading_periods": transform_club_reading_periods,
    "club_badges": transform_club_badges,
    "clubs": transform_clubs,
    "user_reads": transform_user_reads,
    "user_roles": transform_user_roles,
    "user_badges": transform_user_badges,
    "user_permissions": transform_user_permissions,
    "users": transform_users,
    "books": transform_books,
    "book_versions": transform_book_versions,
    "book_series": transform_book_series,
    "creators": transform_creators,
    "awards": transform_awards,
    "formats": transform_formats,
    "languages": transform_languages,
    "creator_roles": transform_creator_roles,
    "read_statuses": transform_read_statuses,
    "countries": transform_countries,
    "genres": transform_genres,
    "publishers": transform_publishers,
    "tags": transform_tags,
}
