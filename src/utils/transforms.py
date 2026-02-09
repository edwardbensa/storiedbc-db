"""Transform utility functions"""

# Imports
import os
import json
from pathlib import Path
from loguru import logger
from src.config import STAGING_COLL_DIR, MAIN_COLL_DIR
from .fields import generate_rlog, compute_d2r, compute_rr, find_doc
from .parsers import clean_document, safe_value


def transform_collection(collection_name: str, transform_func, *, context):
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


def remove_custom_ids(collections_to_cleanup: dict, source_directory):
    """
    Removes specified custom ID fields from each collection in the source directory,
    and writes the cleaned output to MAIN_COLL_DIR.
    """
    source_directory = Path(source_directory)
    output_directory = Path(MAIN_COLL_DIR)

    for collection_name, id_field in collections_to_cleanup.items():
        input_path = source_directory / f"{collection_name}.json"
        output_path = output_directory / f"{collection_name}.json"

        try:
            with input_path.open(encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} documents from '{input_path.name}'")

            cleaned = []
            for doc in data:
                doc.pop(id_field, None)
                doc, _ = clean_document(doc, remove_ts=True)
                cleaned.append(doc)

            with output_path.open("w", encoding="utf-8") as f:
                json.dump(cleaned, f, ensure_ascii=False, indent=2)

            logger.success(f"Removed '{id_field}' from all documents in '{collection_name}.json'")

        except FileNotFoundError:
            logger.warning(f"File not found: {input_path}")
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to process '{input_path.name}': {e}")


def set_custom_ids(collections: dict):
    """
    Replaces the _id field in each document with the value from the specified custom ID field.
    Reads from source_directory and writes to MAIN_COLL_DIR.
    """
    transformed_dir = Path(MAIN_COLL_DIR)
    raw_dir = Path(STAGING_COLL_DIR)

    for collection_name, custom_id_field in collections.items():
        transformed_path = transformed_dir / f"{collection_name}.json"
        raw_path = raw_dir / f"{collection_name}.json"

        if transformed_path.exists():
            input_path = transformed_path
        else:
            logger.info(f"Transforming raw file: {raw_path}...")
            input_path = raw_path

        output_path = transformed_path

        try:
            with input_path.open(encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} documents from '{input_path.name}'")

            updated = []
            for doc in data:
                doc, _ = clean_document(doc, remove_ts=True)
                if custom_id_field in doc:
                    doc["_id"] = str(doc[custom_id_field])
                    doc.pop(custom_id_field, None)
                updated.append(doc)

            with output_path.open("w", encoding="utf-8") as f:
                json.dump(updated, f, ensure_ascii=False, indent=2)

            logger.success(f"Set _id to '{custom_id_field}' in all '{collection_name}' documents.")

        except FileNotFoundError:
            logger.warning(f"File not found: {input_path}")
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to process '{input_path.name}': {e}")


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
