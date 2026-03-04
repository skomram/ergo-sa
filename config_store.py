"""
Persistence for installation data (file-based).
Na produkcji zastapic PostgreSQL / Redis.

Stores per installation:
- shared_secret (from handshake) - encrypted with Fernet
- api_url (from JWT claims)
- configuration (array of step configs)

IMPORTANT: Uses threading locks + file locks to prevent race conditions
when Ergonode sends multiple configuration steps concurrently.
"""
import json
import os
import re
import logging
import fcntl
import base64
import hashlib
import threading
from typing import Optional

logger = logging.getLogger("sellasist-app.store")
STORAGE_DIR = os.getenv("STORAGE_DIR", "/tmp/sellasist_installations")

# ---------------------------------------------------------------------------
# Simple encryption for shared_secret at rest
# ---------------------------------------------------------------------------
_ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")

def _get_fernet():
    try:
        from cryptography.fernet import Fernet
        if _ENCRYPTION_KEY:
            key = base64.urlsafe_b64encode(
                hashlib.sha256(_ENCRYPTION_KEY.encode()).digest())
            return Fernet(key)
        else:
            logger.warning(
                "[STORE] ENCRYPTION_KEY not set - shared_secret stored "
                "as plaintext. Set ENCRYPTION_KEY for production!")
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
    f = _cipher()
    if f and value:
        return "enc:" + f.encrypt(value.encode()).decode()
    return value

def _decrypt_secret(value: str) -> str:
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
        self._locks = {}
        self._locks_lock = threading.Lock()

    def _get_lock(self, installation_id: str) -> threading.Lock:
        with self._locks_lock:
            if installation_id not in self._locks:
                self._locks[installation_id] = threading.Lock()
            return self._locks[installation_id]

    def _path(self, installation_id: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_-]', '_', installation_id)
        if not safe or safe == '_':
            safe = "unknown"
        return os.path.join(STORAGE_DIR, f"{safe}.json")

    def _read_raw(self, path: str) -> dict:
        """Read file without decryption (internal use under lock)."""
        if not os.path.exists(path):
            return {}
        with open(path) as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            return data

    def _write_raw(self, path: str, data: dict):
        """Write file with encryption (internal use under lock)."""
        save_data = dict(data)
        if "shared_secret" in save_data and save_data["shared_secret"]:
            raw = save_data["shared_secret"]
            if not raw.startswith("enc:"):
                save_data["shared_secret"] = _encrypt_secret(raw)
        with open(path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(save_data, f, ensure_ascii=False, indent=2)
            fcntl.flock(f, fcntl.LOCK_UN)

    def _decrypt_data(self, data: dict) -> dict:
        """Decrypt shared_secret in loaded data."""
        if "shared_secret" in data:
            data["shared_secret"] = _decrypt_secret(data["shared_secret"])
        return data

    def save_installation(self, installation_id: str, data: dict):
        """Save full installation data. Use update_config_step for configs."""
        lock = self._get_lock(installation_id)
        path = self._path(installation_id)
        with lock:
            try:
                self._write_raw(path, data)
                logger.info(f"[STORE] Saved {installation_id}")
            except Exception as e:
                logger.error(f"[STORE] Save error {installation_id}: {e}")

    def update_config_step(self, installation_id: str, index: int,
                           config: dict):
        """
        Atomic read-modify-write for a single configuration step.

        Ergonode sends all config steps nearly simultaneously when user
        clicks "Save configuration". Without atomic per-step updates,
        concurrent writes cause data loss (step 2 saves before step 0
        finishes HTTP validation, then step 0 overwrites step 2).
        """
        lock = self._get_lock(installation_id)
        path = self._path(installation_id)
        with lock:
            try:
                inst = self._read_raw(path)
                inst = self._decrypt_data(inst)

                configs = inst.get("configuration", [])
                while len(configs) <= index:
                    configs.append({})
                configs[index] = config
                inst["configuration"] = configs

                self._write_raw(path, inst)
                logger.info(
                    f"[STORE] Updated step {index} for {installation_id}")
            except Exception as e:
                logger.error(
                    f"[STORE] Update step error {installation_id}: {e}")

    def get_installation(self, installation_id: str) -> Optional[dict]:
        lock = self._get_lock(installation_id)
        path = self._path(installation_id)
        if not os.path.exists(path):
            return None
        with lock:
            try:
                data = self._read_raw(path)
                return self._decrypt_data(data)
            except Exception as e:
                logger.error(f"[STORE] Load error {installation_id}: {e}")
                return None

    def remove_installation(self, installation_id: str):
        lock = self._get_lock(installation_id)
        path = self._path(installation_id)
        with lock:
            if os.path.exists(path):
                os.remove(path)
                logger.info(f"[STORE] Removed {installation_id}")
