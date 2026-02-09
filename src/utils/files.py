"""File management utility functions"""

# Imports
import os
import shutil
import hashlib
from loguru import logger


def wipe_directory(directory):
    """
    Deletes all files from specified directory.
    """
    logger.info(f"Checking for existing files in '{directory}'...")
    try:
        files = os.listdir(directory)
        if files:
            logger.info(f"Found {len(files)} existing files. Deleting them now...")
            shutil.rmtree(directory)
            os.makedirs(directory)
            logger.success("Successfully deleted all existing files.")
        else:
            logger.info("Directory is empty. No deletion needed.")
    except OSError as e:
        logger.error(f"Error deleting files from '{directory}': {e}")
        exit()


def generate_image_filename(doc: dict, img_type: str):
    """
    Generate a hashed filename for a profile image using a unique field entry.
    Also generates a hashed filename for a book cover using the first valid unique identifier
    where prefix is based on the index of the identifier in the list.
    """
    if img_type not in ["user", "club", "cover", "creator"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    hash_length=20
    if img_type == "cover":
        unique_fields=['isbn_13', 'asin']
        for index, unique_field in enumerate(unique_fields):
            value = str(doc.get(unique_field))
            if value and isinstance(value, str) and value.strip():
                hash_digest = hashlib.sha256(value.encode()).hexdigest()[:hash_length]
                prefix = f"b{str(index + 1).zfill(2)}"
                return f"{prefix}-{hash_digest}"

        raise ValueError("No valid unique identifier found in book metadata.")
    elif img_type in ["user", "club"]:
        field = doc.get(f"{img_type}_handle")
        if field and isinstance(field, str) and field.strip():
            hash_digest = hashlib.sha256(field.encode()).hexdigest()[:hash_length]
            return f"{hash_digest}.jpg"
    else:
        field = doc.get("profile_photo")
        if field and isinstance(field, str) and field.strip():
            hash_digest = hashlib.sha256(field.encode()).hexdigest()[:hash_length]
            return f"{hash_digest}.jpg"

        raise ValueError("Missing or invalid handle for profile photo.")
