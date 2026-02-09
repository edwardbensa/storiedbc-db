"""Connection utility functions"""

# Imports
import sys
import time
import functools
from typing import Tuple
from datetime import datetime
import gspread
import requests
from requests.exceptions import RequestException
from oauth2client.service_account import ServiceAccountCredentials
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from pymongo import MongoClient
from pymongo.errors import PyMongoError, AutoReconnect, NetworkTimeout, ExecutionTimeout
from pymongo.errors import ConnectionFailure, ConfigurationError
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from loguru import logger
from src.config import (gsheet_cred, mongodb_uri, azure_str,
                        neo4j_uri, neo4j_user, neo4j_pwd)


def connect_mongodb(db_name="main"):
    """
    Connects to MongoDB and returns specified database object.
    """
    if db_name not in ["main", "staging", "etl_metadata"]:
        raise ValueError("The database name must be 'main', 'staging', or 'etl_metadata'.")
    try:
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB: {db_name} database.")
        return db
    except (ConnectionFailure, ConfigurationError)  as e:
        logger.error(f"Failed to connect to MongoDB: {e}.")
        sys.exit()

def close_mongodb():
    """Close MongoDB client."""
    client = MongoClient(mongodb_uri)
    client.close()
    logger.info("MongoDB connection closed.")

def connect_auradb():
    """
    Connects to Neo4j AuraDB and creates a driver.
    """
    if neo4j_uri is None or neo4j_user is None or neo4j_pwd is None:
        raise ValueError("neo4j_user and neo4j_pwd must not be None")
    uri = neo4j_uri
    auth = (neo4j_user, neo4j_pwd)

    try:
        driver = GraphDatabase.driver(uri, auth=auth)
        with driver.session() as session:
            session.run("RETURN 1")
        logger.info("Successfully connected to AuraDB")
        return driver
    except (ServiceUnavailable, AuthError) as e:
        logger.error(f"Failed to connect to AuraDB: {e}")
        sys.exit()


def connect_azure_blob():
    """
    Connects to Azure Blob Storage and returns BlobServiceClient.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_str) # type: ignore
        logger.info("Successfully connected to Azure Blob Storage.")
        return blob_service_client
    except (ConnectionFailure, ConfigurationError) as e:
        logger.error(f"Failed to connect to Azure Blob Storage: {e}")
        sys.exit()


def make_blob_public(container_client, blob_name):
    """
    Sets the access level of a blob to public read.
    """
    try:
        acl = container_client.get_container_access_policy()
        if acl.get('public_access') != 'blob':
            container_client.set_container_access_policy(public_access='blob')
            logger.info(f"Set container access policy to public for blob '{blob_name}'.")
        else:
            logger.info(f"Blob '{blob_name}' is already public.")
    except (KeyError, TypeError, ValueError, AzureError) as e:
        logger.error(f"Failed to set blob '{blob_name}' to public: {e}")


def connect_gsheet():
    """
    Connects to Book Club DB and returns the spreadsheet.
    """
    try:
        # Google Sheets authorization
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(gsheet_cred, scope)  # type: ignore
        client_sheet = gspread.authorize(creds) # type: ignore

        # Open spreadsheet
        spreadsheet = client_sheet.open("Book Club DB")
        logger.info("Connected to Google Sheet")
        return spreadsheet
    except gspread.exceptions.APIError as e:
        logger.error(f"APIError for sheet 'Book Club DB': {e}")
        raise


def wipe_container(blob_service_client, container_name: str,
                   raise_on_error: bool = True) -> Tuple[bool, int]:
    """
    Deletes all blobs from the specified Azure Blob Storage container.
    
    Args:
        container_name: Name of the container to clear
        raise_on_error: Whether to raise exceptions or return error status
        
    Returns:
        Tuple of (success: bool, deleted_count: int)
        
    Raises:
        AzureError: If raise_on_error is True and deletion fails
        ValueError: If container_name is empty or None
    """
    if not container_name or not container_name.strip():
        raise ValueError("Container name cannot be empty or None")

    deleted_count = 0

    try:
        container_client = blob_service_client.get_container_client(container_name)

        # Check if container exists
        if not container_client.exists():
            logger.warning(f"Container '{container_name}' does not exist")
            return False, 0

        logger.info(f"Starting deletion of blobs from container '{container_name}'...")

        # Get blob list iterator (more memory efficient)
        blob_pages = container_client.list_blobs().by_page()

        for page in blob_pages:
            batch_blobs = list(page)
            if not batch_blobs:
                break

            logger.debug(f"Processing batch of {len(batch_blobs)} blobs")

            # Delete blobs in current batch
            for blob in batch_blobs:
                try:
                    container_client.delete_blob(blob.name, delete_snapshots="include")
                    deleted_count += 1
                    logger.debug(f"Deleted blob: {blob.name}")
                except ResourceNotFoundError:
                    # Blob might have been deleted by another process
                    logger.debug(f"Blob already deleted: {blob.name}")
                except AzureError as blob_error:
                    logger.error(f"Failed to delete blob '{blob.name}': {blob_error}")
                    if raise_on_error:
                        raise

        if deleted_count > 0:
            logger.info(f"Successfully deleted {deleted_count} blobs from '{container_name}'")
        else:
            logger.info(f"Container '{container_name}' was already empty")

        return True, deleted_count

    except AzureError as e:
        error_msg = f"Failed to delete blobs from container '{container_name}': {e}"
        logger.error(error_msg)

        if raise_on_error:
            raise AzureError(error_msg) from e
        return False, deleted_count


def sync_images(db, blob_service_client, container_name, img_type, url_map):
    """
    Syncs images directly from source URLs to Azure Blob Storage container.
    Uploads new files and deletes obsolete files based on log comparisons.
    URL map: { filename: source_url }
    """
    if img_type not in ["user", "club", "cover"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")
    img_type_map = {"user": "user_dp", "club": "club_dp", "cover": "cover_art"}
    img_type = img_type_map[img_type]

    container_client = blob_service_client.get_container_client(container_name)

    # Fetch known state from MongoDB
    log_doc = db["metadata"].find_one({"_id": f"inventory_{img_type}"})
    known_filenames = set(log_doc["filenames"]) if log_doc else set()
    desired_filenames = set(url_map.keys())

    # Identify deltas
    to_upload = desired_filenames - known_filenames
    to_delete = known_filenames - desired_filenames

    # Handle deletions in Azure
    try:
        for filename in to_delete:
            try:
                container_client.delete_blob(filename)
                logger.info(f"Deleted obsolete blob: {filename}")
            except ResourceNotFoundError:
                pass # Already gone

        # Stream uploads to Azure (source -> memory -> Azure)
        for filename in to_upload:
            source_url = url_map[filename]
            try:
                # Stream the image into memory
                response = requests.get(source_url, stream=True, timeout=15)
                if response.status_code == 200:
                    blob_client = container_client.get_blob_client(filename)
                    # upload_blob accepts the raw response stream
                    blob_client.upload_blob(response.raw, overwrite=True)
                    logger.success(f"Streamed and uploaded: {filename}")
                else:
                    logger.warning(f"Failed to fetch: {source_url} (HTTP {response.status_code})")
            except (RequestException, AzureError) as e:
                logger.error(f"Error streaming {filename}: {e}")

        # Update known state in MongoDB
        db["metadata"].update_one(
            {"_id": f"inventory_{img_type}"},
            {"$set": {
                "filenames": list(desired_filenames),
                "updated_at": datetime.now()
            }},
            upsert=True
        )
    except AzureError as e:
        logger.error(f"Critical error accessing container '{container_name}': {e}")
        sys.exit()


RETRYABLE_ERRORS = (
    PyMongoError,
    gspread.exceptions.APIError,
    ConnectionError,
    TimeoutError,
    AutoReconnect,
    NetworkTimeout,
    ExecutionTimeout,
)

def retry(max_attempts=3, backoff=1.5):
    """Retry function decorator."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            retry_count = 0

            while True:
                try:
                    result = func(*args, **kwargs)
                    # attach retry count to the result if it's a dict
                    if isinstance(result, dict):
                        result["_retry_count"] = retry_count
                    return result

                except RETRYABLE_ERRORS as exc:
                    if attempt >= max_attempts:
                        raise

                    retry_count += 1
                    wait = backoff ** attempt

                    logger.warning(
                        f"Retryable error in {func.__name__}: {exc}. "
                        f"Retrying in {wait:.1f}s (attempt {attempt}/{max_attempts})"
                    )

                    time.sleep(wait)
                    attempt += 1

        return wrapper
    return decorator
