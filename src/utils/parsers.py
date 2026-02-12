"""Parsing utility functions"""

# Import modules
import json
import hashlib
from datetime import datetime
from collections import defaultdict
from bson.errors import InvalidId
from bson import ObjectId
from loguru import logger


def clean_document(doc: dict, remove_ts: bool = False):
    """
    Removes keys with None, empty lists, or empty strings from a document.
    Also removes 'full_hash' and 'id_hash'. Optionally removes 'updated_at'.
    """
    # Keys to always remove
    keys_to_remove = {"full_hash", "id_hash"}

    # Optionally remove updated_at
    if remove_ts:
        keys_to_remove.add("updated_at")

    clean_doc = {
        k: v for k, v in doc.items()
        if v is not None and v != [] and v != '' and k not in keys_to_remove
    }

    old_keys = list(doc.keys())
    new_keys = list(clean_doc.keys())
    removed_keys = [k for k in old_keys if k not in new_keys]

    return clean_doc, removed_keys


def to_datetime(date_string, verbose=True):
    """
    Converts a date string to a datetime object.
    Returns None if the input is None, empty, or invalid.
    """
    if not date_string or not isinstance(date_string, str):
        return None

    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%d/%m/%Y',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_string.strip(), fmt)
        except ValueError:
            continue

    if verbose:
        logger.error(f"Invalid date format for '{date_string}'")
    return None


def to_int(value):
    """
    Converts a value to an integer. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return int(value)
    except ValueError as e:
        logger.error(f"Invalid integer value '{value}': {e}")
        return None


def to_float(value):
    """
    Converts a value to a float. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return float(value)
    except ValueError as e:
        logger.error(f"Invalid float value '{value}': {e}")
        return None


def to_array(field_string):
    """
    Converts a comma-separated string into a list of trimmed strings.
    """
    if not field_string:
        return []
    return [item.strip() for item in field_string.split(',') if item.strip()]


def make_subdocuments(string: str, field_key: str, registry, separator = ';'):
    """
    Parses a separator-separated string into a list of subdocuments
    using the pattern and transform function defined in the subdoc_registry.
    """
    if not string:
        return []

    if string == '':
        return []

    config = registry.get(field_key)
    if not config or not callable(config.get('transform')):
        logger.error(f"Invalid subdocument config for field '{field_key}'")
        return []

    pattern = config.get('pattern')
    transform = config['transform']

    entries = [entry.strip() for entry in string.split(separator) if entry.strip()]

    if pattern:
        transformed_list = []
        for entry in entries:
            match = pattern.match(entry)
            if match:
                transformed_list.append(transform(match))
            else:
                logger.warning(f"No match for entry: '{entry}' in field '{field_key}'")
        return transformed_list

    return [transform(entry) for entry in entries]


def make_array(string: str, field_key: str, registry, separator=';'):
    """
    Parses a separator-separated string into a list of transformed values
    using the transform function defined in the registry.
    """
    if not string:
        return []

    config = registry.get(field_key)
    if not config or not callable(config.get('transform')):
        logger.error(f"Invalid array config for field '{field_key}'")
        return []

    transform = config['transform']
    entries = [entry.strip() for entry in string.split(separator) if entry.strip()]
    return [transform(entry) for entry in entries]


def hash_doc(doc: dict):
    """Hash a dict deterministically."""
    return hashlib.sha1(json.dumps(safe_value(doc), sort_keys=True).encode()).hexdigest()


def add_hashes(documents, id_fields):
    """
    Add deterministic hashes to documents based on full content,
    plus identity hash for matching.
    """
    mod_docs = []

    for doc in documents:
        # Extract full content and identity data
        ignore_keys = ("_id", "full_hash", "id_hash", "updated_at")
        full_data = {k: v for k, v in doc.items() if k not in ignore_keys}
        identity_data = {k: full_data[k] for k in id_fields} if id_fields else {}

        # Add hashes
        mod_doc = dict(doc)
        mod_doc["full_hash"] = hash_doc(full_data)
        mod_doc["id_hash"] = hash_doc(identity_data)
        mod_docs.append(mod_doc)

    id_hashes = [d["id_hash"] for d in mod_docs]
    full_hashes = [d["full_hash"] for d in mod_docs]
    return mod_docs, id_hashes, full_hashes


def find_deltas(old_documents, new_documents):
    """
    Categorises records into New, Updated, Deleted, and Unchanged.

    Returns:
        to_upsert: List of new and changed docs to write to DB.
        diff: Metadata about what happened for removals/logging/stats.
    """

    # Index new docs by id_hash
    new_lookup = {doc["id_hash"]: doc for doc in new_documents}

    to_upsert = []
    diff = {"new": [], "updated": [], "unchanged": [], "deleted": []}
    matched_new_hashes = set()

    ignore_keys = ("_id", "full_hash", "id_hash", "updated_at")

    # Compare old to new
    for old_doc in old_documents:
        id_hash = old_doc["id_hash"]
        new_doc = new_lookup.get(id_hash)

        # No matching new doc -> deleted
        if new_doc is None:
            diff["deleted"].append(old_doc)
            continue

        matched_new_hashes.add(id_hash)

        # If full_hash matches, treat as unchanged
        if old_doc.get("full_hash") == new_doc.get("full_hash"):
            diff["unchanged"].append(old_doc)
            continue

        # Compute field‑level changes (symmetric)
        changes = {}
        all_keys = set(old_doc.keys()) | set(new_doc.keys())

        for key in all_keys:
            if key in ignore_keys:
                continue

            old_val = old_doc.get(key)
            new_val = new_doc.get(key)

            if old_val != new_val:
                changes[key] = {"from": old_val, "to": new_val}

        # Build updated doc, preserving old _id
        clean_new = {k: v for k, v in new_doc.items() if k != "_id"}
        updated_doc = {"_id": old_doc["_id"], **clean_new}

        to_upsert.append(updated_doc)
        diff["updated"].append({
            "_id": old_doc["_id"],
            "id_hash": id_hash,
            "changes": changes
        })

    # Find new records
    for new_doc in new_documents:
        if new_doc["id_hash"] not in matched_new_hashes:
            new_entry = {"_id": ObjectId(), **new_doc}
            to_upsert.append(new_entry)
            diff["new"].append(new_entry)

    return to_upsert, diff


def safe_value(v):
    """Recursively convert ObjectIds to strings and datetime to ISO format."""
    if isinstance(v, list):
        return [safe_value(item) for item in v]

    if isinstance(v, dict):
        return {k: safe_value(val) for k, val in v.items()}

    if isinstance(v, ObjectId):
        return str(v)

    if isinstance(v, datetime):
        return v.isoformat()

    return v


def remove_nested_dicts(entry):
    """Remove keys whose values are dicts or lists of dicts."""
    for key in list(entry.keys()):
        value = entry[key]

        if isinstance(value, dict):
            entry.pop(key)
            continue

        if isinstance(value, list) and any(isinstance(i, dict) for i in value):
            entry.pop(key)

    return entry


def flatten_document(entry, field_map):
    """Flatten document using a field map."""
    entry = entry.copy()
    output_lists = defaultdict(list)

    for out_field, path in field_map.items():
        parent, child = path.split(".")
        value = entry.get(parent, [])

        # Parent is a dict
        if isinstance(value, dict):
            if child in value:
                output_lists[out_field].append(safe_value(value[child]))

        # Parent is a list of dicts
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and child in item:
                    output_lists[out_field].append(safe_value(item[child]))

    # Insert flattened fields and remove nested dicts
    entry.update(output_lists)
    reassigned = [k for k, v in field_map.items() if k == v.split(".")[0]]
    field_map = {k: v for k, v in field_map.items() if v.split(".")[0] not in reassigned}
    for path in field_map.values():
        if "." in str(path):
            parent = path.split(".")[0]
            entry.pop(parent, None)

    return entry


def convert_document(doc):
    """Recursively try converting ObjectId or datetime."""
    if isinstance(doc, dict):
        return {k: convert_document(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [convert_document(v) for v in doc]
    if isinstance(doc, str):
        # Try ObjectId
        try:
            return ObjectId(doc)
        except InvalidId:
            pass

        # Try datetime
        dt = to_datetime(doc, verbose=False)
        if dt is not None:
            return dt
    return doc


def id_docs(documents):
    """Ensure _id is ObjectId."""
    cleaned = []
    for doc in documents:
        doc = dict(doc)

        if "_id" in doc:
            # Set _id to ObjectId
            doc["_id"] = ObjectId(doc["_id"])
        else:
            # Add ObjectId
            doc = {"_id": ObjectId(), **doc}

        cleaned.append(doc)
    return cleaned
