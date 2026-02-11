"""Container wipe script"""

# Imports
from loguru import logger
from src.utils.connectors import connect_azure_blob, wipe_container


# Connect to Azure Blob Storage
blob_service_client = connect_azure_blob()
CONTAINER_NAME = 'cover-art'


if __name__ == "__main__":
    logger.info("Wiping Azure container...")
    wipe_container(blob_service_client, CONTAINER_NAME)
    logger.info("Wiped Azure container.")
