"""
Persistence for installation data (file-based).
Na produkcji zastapic PostgreSQL / Redis.

Stores per installation:
- shared_secret (from handshake) - encrypted with Fernet
- api_url (from JWT claims)
- configuration (array of step configs)
"""
import json
import os
import re
import logging
import fcntl
import base64
import hashlib
from typing import Optional

logger = logging.getLogger("sellasist-app.store")
STORAGE_DIR = os.getenv("STORAGE_DIR", "/tmp/sellasist_installations")

# ---------------------------------------------------------------------------
# Simple encryption for shared_secret at rest
# Uses Fernet-compatible approach with a key derived from ENCRYPTION_KEY env
# or a default (change in production!).
# ---------------------------------------------------------------------------
_ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")

def _get_fernet():
    """Lazy-load Fernet cipher. Returns None if cryptography not available."""
    try:
        from cryptography.fernet import Fernet
        if _ENCRYPTION_KEY:
            # Derive a valid Fernet key from arbitrary string
            key = base64.urlsafe_b64encode(
                hashlib.sha256(_ENCRYPTION_KEY.encode()).digest())
            return Fernet(key)
        else:
            logger.warning(
                "[STORE] ENCRYPTION_KEY not set - shared_secret stored as plaintext. "
                "Set ENCRYPTION_KEY env variable for production!")
            return None
    except ImportError:
        logger.warning(
            "[STORE] cryptography package not installed - "
            "shared_secret stored as plaintext")
        return None

_fernet = None
_fernet_loaded = False

def _cipher():
    global _fernet, _fernet_loaded
    if not _fernet_loaded:
        _fernet = _get_fernet()
        _fernet_loaded = True
    return _fernet


def _encrypt_secret(value: str) -> str:
    """Encrypt a string value. Returns prefixed ciphertext or plaintext."""
    f = _cipher()
    if f and value:
        return "enc:" + f.encrypt(value.encode()).decode()
    return value


def _decrypt_secret(value: str) -> str:
    """Decrypt a string value. Handles both encrypted and plaintext."""
    f = _cipher()
    if f and value and value.startswith("enc:"):
        try:
            return f.decrypt(value[4:].encode()).decode()
        except Exception as e:
            logger.error(f"[STORE] Decrypt error: {e}")
            return ""
    return value


class ConfigStore:
    def __init__(self):
        os.makedirs(STORAGE_DIR, exist_ok=True)

    def _path(self, installation_id: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_-]', '_', installation_id)
        if not safe or safe == '_':
            safe = "unknown"
        return os.path.join(STORAGE_DIR, f"{safe}.json")

    def save_installation(self, installation_id: str, data: dict):
        path = self._path(installation_id)
        try:
            # Encrypt shared_secret before persisting
            save_data = dict(data)
            if "shared_secret" in save_data and save_data["shared_secret"]:
                raw = save_data["shared_secret"]
                if not raw.startswith("enc:"):
                    save_data["shared_secret"] = _encrypt_secret(raw)

            with open(path, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(save_data, f, ensure_ascii=False, indent=2)
                fcntl.flock(f, fcntl.LOCK_UN)
            logger.info(f"[STORE] Saved {installation_id}")
        except Exception as e:
            logger.error(f"[STORE] Save error {installation_id}: {e}")

    def get_installation(self, installation_id: str) -> Optional[dict]:
        path = self._path(installation_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN)

                # Decrypt shared_secret on read
                if "shared_secret" in data:
                    data["shared_secret"] = _decrypt_secret(
                        data["shared_secret"])
                return data
        except Exception as e:
            logger.error(f"[STORE] Load error {installation_id}: {e}")
            return None

    def remove_installation(self, installation_id: str):
        path = self._path(installation_id)
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"[STORE] Removed {installation_id}")
