"""Project config"""

# Imports
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
gsheet_cred = os.getenv("GSHEET_CRED")
mongodb_uri = os.getenv("MONGODB_URI")
azure_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
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

MODELS_DIR = PROJ_ROOT / "models"
REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

SECRETS_DIR = SRC / "secrets"
