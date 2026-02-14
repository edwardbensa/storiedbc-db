"""Security utility functions"""

# Imports
import json
import bcrypt
from cryptography.fernet import Fernet
from src.config import key_registry_path


# Load key registry
if key_registry_path is None:
    raise ValueError("ENCRYPTION_KEYS environment variable is not set.")
with open(key_registry_path, 'r', encoding='utf-8') as f:
    key_registry = json.load(f)

# Use latest key as default
latest_key_version = sorted(key_registry.keys())[-1]
default_cipher = Fernet(key_registry[latest_key_version].encode())


# SECURITY FUNCTIONS

def hash_password(plain_password: str) -> str:
    """
    Hashes and salts passwords.
    """
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(plain_password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(entered_password: str, stored_hash: str) -> bool:
    """
    Password verification for user account access.
    """
    return bcrypt.checkpw(entered_password.encode('utf-8'), stored_hash.encode('utf-8'))


def encrypt_pii(value: str, version: str = latest_key_version) -> str:
    """
    Encrypts PII using latest key.
    """
    if value is None:
        return None
    cipher = Fernet(key_registry[version].encode())
    return cipher.encrypt(value.encode('utf-8')).decode('utf-8')


def decrypt_pii(encrypted_value: str, version: str) -> str:
    """
    Decrypts PII using the correct key version.
    """
    if encrypted_value is None:
        return None
    cipher = Fernet(key_registry[version].encode())
    return cipher.decrypt(encrypted_value.encode('utf-8')).decode('utf-8')


def decrypt_field(value, version):
    """
    Decrypts field using the correct key version.
    """
    if not value or version not in key_registry:
        return value
    cipher = Fernet(key_registry[version].encode())
    return cipher.decrypt(value.encode()).decode()


def encrypt_field(value, version):
    """
    Encrypts field using latest key.
    """
    if not value or version not in key_registry:
        return value
    cipher = Fernet(key_registry[version].encode())
    return cipher.encrypt(value.encode()).decode()
