"""Connection utility functions"""

# Imports
import sys
import time
import functools
from typing import Tuple
from datetime import datetime, timezone
import gspread
import requests
from requests.exceptions import (
    ConnectionError as RequestsConnectionError, Timeout,
    HTTPError, RequestException
)
from oauth2client.service_account import ServiceAccountCredentials
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from pymongo import MongoClient
from pymongo.errors import (
    PyMongoError, AutoReconnect, NetworkTimeout, ExecutionTimeout,
    ConnectionFailure, ConfigurationError
    )
from neo4j import GraphDatabase
from neo4j.exceptions import (
    AuthError, ServiceUnavailable, TransientError, SessionExpired,
    DatabaseUnavailable
)
from loguru import logger
from src.config import (
    gsheet_cred, mongodb_uri, azure_str,
    neo4j_uri, neo4j_user, neo4j_pwd
    )
from .ops_mongo import load_sync_state, update_sync_state


class MongoDBConnection:
    """Singleton MongoDB connection manager."""
    _instances = {}  # {db_name: client}

    @classmethod
    def get_client(cls, db_name="main"):
        """Get or create MongoDB client for the specified database."""
        if db_name not in ["main", "staging", "etl_metadata"]:
            raise ValueError(f"Invalid db_name: {db_name}")

        if db_name not in cls._instances:
            try:
                client = MongoClient(mongodb_uri)
                client.admin.command('ping')
                cls._instances[db_name] = client
                logger.info(f"Created MongoDB connection: {db_name}")
            except (ConnectionFailure, ConfigurationError) as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                sys.exit(1)

        return cls._instances[db_name]

    @classmethod
    def get_database(cls, db_name="main"):
        """Get database object."""
        client = cls.get_client(db_name)
        return client[db_name]

    @classmethod
    def close_all(cls):
        """Close all open connections."""
        for db_name, client in cls._instances.items():
            try:
                client.close()
                logger.info(f"Closed MongoDB connection: {db_name}")
            except Exception as e: # pylint: disable=W0718
                logger.error(f"Error closing {db_name}: {e}")
        cls._instances.clear()


def connect_mongodb(db_name="main"):
    """Connects to MongoDB and returns specified database object."""
    return MongoDBConnection.get_database(db_name)


def close_mongodb():
    """Close all MongoDB connections."""
    MongoDBConnection.close_all()


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


def sync_images(etl_db, blob_service_client, img_type, container_name):
    """
    Incrementally sync images to Azure Blob Storage based on per-file inventory docs.

    Uploads:
        inventory_images where:
            img_type = img_type AND
            deleted = False AND
            updated_at > last_sync_time

    Deletions:
        inventory_images where:
            img_type = img_type AND
            deleted = True AND
            updated_at > last_sync_time
    """

    # Load sync state
    sync_id = f"sync_{img_type}"
    lst = load_sync_state(etl_db, sync_id)

    container = blob_service_client.get_container_client(container_name)

    # Query inventory for uploads
    to_upload = list(etl_db["inventory_images"].find({
        "img_type": img_type,
        "deleted": False,
        "updated_at": {"$gt": lst}
    }))

    # Query inventory for deletions
    to_delete = list(etl_db["inventory_images"].find({
        "img_type": img_type,
        "deleted": True,
        "updated_at": {"$gt": lst}
    }))

    logger.info(f"{len(to_upload)} uploads, {len(to_delete)} deletions pending for {img_type}")

    # Perform deletions
    for item in to_delete:
        filename = item["_id"]
        try:
            container.delete_blob(filename)
            logger.info(f"Deleted blob: {filename}")
        except ResourceNotFoundError:
            pass
        except AzureError as e:
            logger.error(f"Azure deletion error for {filename}: {e}")

    # Perform uploads
    for item in to_upload:
        filename = item["_id"]
        source_url = item["source_url"]

        try:
            response = requests.get(source_url, stream=True, timeout=15)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {source_url} (HTTP {response.status_code})")
                continue

            blob = container.get_blob_client(filename)
            blob.upload_blob(response.raw, overwrite=True)
            logger.success(f"Uploaded: {filename}")

        except (RequestException, AzureError) as e:
            logger.error(f"Upload error for {filename}: {e}")

    # Update sync state
    update_sync_state(
        etl_db,
        sync_id,
        timestamp=datetime.now(timezone.utc),
        batch_id=time.strftime("%Y%m%d-%H%M%S"))

    logger.success(f"Sync complete for {img_type}. Updated sync state.")


RETRYABLE_ERRORS = (
    # MongoDB
    PyMongoError,
    AutoReconnect,
    NetworkTimeout,
    ExecutionTimeout,

    # Neo4j / AuraDB
    ServiceUnavailable,
    TransientError,
    SessionExpired,
    DatabaseUnavailable,

    # Google Sheets
    gspread.exceptions.APIError,

    # HTTP / Network
    ConnectionError,
    RequestsConnectionError,
    TimeoutError,
    Timeout,
    HTTPError,  # Only retry on 5xx errors

    # OS-level
    OSError,  # Socket errors, file descriptor issues
)

def retry(max_attempts=3, backoff=1.5):
    """Retry function decorator with smart HTTP error handling."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            retry_count = 0

            while True:
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, dict):
                        result["_retry_count"] = retry_count
                    return result

                except HTTPError as exc:
                    # Only retry server errors (5xx), not client errors (4xx)
                    if exc.response is not None and 500 <= exc.response.status_code < 600:
                        if attempt >= max_attempts:
                            raise
                        # Proceed to retry logic below
                    else:
                        # Don't retry 4xx errors (bad request, auth, etc.)
                        raise

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
