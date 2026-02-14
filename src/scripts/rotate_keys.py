"""Key rotation script"""

# Imports
from loguru import logger
from pymongo.errors import ConnectionFailure, ConfigurationError
from src.utils.connectors import connect_mongodb
from src.utils.security import decrypt_field, encrypt_field, latest_key_version


# Connect to MongoDB
db = connect_mongodb()
users = db["users"]

# Fields to rotate
encrypted_fields = [
    "user_emailaddress", "user_dob", "user_gender",
    "user_city", "user_state", "user_country"
]


def rotate_user_document(doc):
    """
    Decrypts and encrypts user document with new key version.
    """
    old_key_version = doc.get("key_version")
    if old_key_version == latest_key_version:
        return False  # Already up to date

    updated_fields = {}
    for field in encrypted_fields:
        if field in doc:
            try:
                decrypted = decrypt_field(doc[field], old_key_version)
                re_encrypted = encrypt_field(decrypted, latest_key_version)
                updated_fields[field] = re_encrypted
            except (KeyError, TypeError, ValueError, ConnectionFailure, ConfigurationError) as e:
                logger.warning(f"Failed to rotate field '{field}' for user {doc.get('_id')}: {e}")

    if updated_fields:
        updated_fields["key_version"] = latest_key_version
        users.update_one({"_id": doc["_id"]}, {"$set": updated_fields})
        logger.info(f"Rotated user {doc.get('_id')} to key version '{latest_key_version}'")
        return True
    return False


def rotate_all_users():
    """
    Rotates encryption for all user documents.
    """
    outdated_users = users.find({"key_version": {"$ne": latest_key_version}})
    count = 0
    for user in outdated_users:
        if rotate_user_document(user):
            count += 1
    logger.info(f"Rotated {count} user documents to key version '{latest_key_version}'")


if __name__ == "__main__":
    rotate_all_users()
