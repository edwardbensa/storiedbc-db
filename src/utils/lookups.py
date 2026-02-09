"""Lookup utlity functions"""

# Imports
from loguru import logger
from .parsers import to_int


# Load lookup collections
def load_lookup_data(db, lookup_registry: dict) -> dict:
    """
    Builds lookup maps by fetching directly from MongoDB.
    """
    lookup_data = {}

    for name, config in lookup_registry.items():
        # Access the collection in the staging database
        collection = db[name]
        string_field = config["field"]
        get_fields = config["get"]

        # Build projection for efficiency
        projection = {string_field: 1}
        if isinstance(get_fields, str):
            projection[get_fields] = 1
        else:
            for f in get_fields:
                projection[f] = 1

        # Fetch the full lookup set from MongoDB
        cursor = collection.find({}, projection)

        if isinstance(get_fields, str):
            lookup_data[name] = {
                doc[string_field]: doc.get(get_fields)
                for doc in cursor if string_field in doc
            }
        else:
            lookup_data[name] = {
                doc[string_field]: {field: doc.get(field) for field in get_fields}
                for doc in cursor if string_field in doc
            }

        logger.debug(f"Loaded {len(lookup_data[name])} entries into '{name}' lookup map.")

    return lookup_data


def resolve_lookup(collection_name, input_string, lookup_data):
    """
    Uses a lookup registry to return specified fields.

    Args:
        collection_name: The name of the collection to search.
        input_string: The value to search for.
        registry: The lookup configuration registry.

    Returns:
        If 'get' is a string, returns a single value.
        If 'get' is a list, returns a dictionary of values.
        Returns None if no match is found or configuration is incomplete.
    """
    return lookup_data.get(collection_name, {}).get(input_string)


def resolve_creator(creator_id: str, lookup_data) -> dict:
    """
    Resolves a creator by custom creator_id and returns:
    - _id: MongoDB ObjectId
    - {creator_role}_name: Full name (firstname + lastname)
    """
    doc = lookup_data["creators"].get(creator_id)
    if not doc:
        logger.warning(f"No creator found for ID '{creator_id}'")
        return {}
    full_name = f"{doc.get('firstname', '').strip()} {doc.get('lastname', '').strip()}"
    return {
        "_id": doc["_id"],
        "name": full_name
    }


def resolve_awards(match, lookup_data: dict) -> dict:
    """
    Resolves award subdocument from regex match groups.
    Omits award_category if category ID is ''.
    """

    if  match.group(3) == '':
        subdoc = {
            "_id": resolve_lookup('awards', match.group(1), lookup_data),
            "name": match.group(2),
            "year": to_int(match.group(4)),
            "status": match.group(5)
        }
    else:
        subdoc = {
                "_id": resolve_lookup('awards', match.group(1), lookup_data),
                "name": match.group(2),
                "category": match.group(3),
                "year": to_int(match.group(4)),
                "status": match.group(5)
            }

    return subdoc


def find_doc(docs: list, key: str, value) -> dict:
    """
    Find single dict in list of dicts

    Search for first dict in docs where the value for 'key' is 'value',
    Returns an empty dict if no match is found.
    """
    for doc in docs:
        if doc.get(key) == value:
            return doc
    return {}
