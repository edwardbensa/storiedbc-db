"""Project config"""

# Imports
import os
import base64
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

# Reconstruct secrets
def bootstrap_secrets():
    """Checks for secret files and decodes them from Base64 if missing."""
    # Define the mapping of Env Var -> Target File Path
    secret_map = {
        "ENV_FILE_BASE64": ".env",
        "GSHEET_JSON_BASE64": "src/secrets/gsheets_creds.json",
        "KEYS_JSON_BASE64": "src/secrets/key_registry.json"
    }

    for env_var, file_path_str in secret_map.items():
        file_path = Path(file_path_str)

        # Only decode if the file doesn't already exist
        if not file_path.exists():
            encoded_content = os.getenv(env_var)

            if encoded_content:
                try:
                    logger.info(f"Decoding {env_var} into {file_path_str}...")
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    decoded_data = base64.b64decode(encoded_content)
                    file_path.write_bytes(decoded_data)
                except Exception as e: # pylint: disable=W0718
                    logger.error(f"Failed to decode {env_var}: {e}")
            else:
                logger.debug(f"Secret file {file_path_str} missing and no Base64 env var found.")


# Load environment variables
load_dotenv()
gsheet_cred = os.getenv("GSHEET_CREDS")
mongodb_uri = os.getenv("MONGODB_URI")
azure_conte = os.getenv("AZURE_CONTE_CS")
neo4j_uri = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USERNAME")
neo4j_pwd = os.getenv("NEO4J_PASSWORD")
key_registry_path = os.getenv("ENCRYPTION_KEYS")
hf_token = os.getenv("HUGGINGFACE_HUB_TOKEN")

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJ_ROOT / "src"
DATA_DIR = PROJ_ROOT / "data"
STAGING_COLL_DIR = DATA_DIR / "staging"
MAIN_COLL_DIR = DATA_DIR / "main"
AURA_COLL_DIR = DATA_DIR / "aura"
SECRETS_DIR = SRC / "secrets"
