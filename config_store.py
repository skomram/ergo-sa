"""
Persistence for installation data.
UWAGA: Plikowy storage - na produkcji zastapic PostgreSQL / Redis.

Przechowuje:
- shared_secret (z handshake)
- configuration (tablica kroków konfiguracji)
- ergonode_api_url
- installed flag
"""
import json
import os
import re
import logging
import fcntl

logger = logging.getLogger("sellasist-app.store")
STORAGE_DIR = os.getenv("STORAGE_DIR", "/tmp/sellasist_installations")


class ConfigStore:
    def __init__(self):
        os.makedirs(STORAGE_DIR, exist_ok=True)

    def _path(self, installation_id: str) -> str:
        safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', installation_id)
        if not safe_id or safe_id == '_':
            safe_id = "unknown"
        return os.path.join(STORAGE_DIR, f"{safe_id}.json")

    def save_installation(self, installation_id: str, data: dict):
        path = self._path(installation_id)
        try:
            with open(path, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(data, f, ensure_ascii=False, indent=2)
                fcntl.flock(f, fcntl.LOCK_UN)
            logger.info(f"[STORE] Saved installation {installation_id}")
        except Exception as e:
            logger.error(f"[STORE] Error saving {installation_id}: {e}")

    def get_installation(self, installation_id: str) -> dict:
        path = self._path(installation_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN)
                return data
        except Exception as e:
            logger.error(f"[STORE] Error loading {installation_id}: {e}")
            return None

    def remove_installation(self, installation_id: str):
        path = self._path(installation_id)
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"[STORE] Removed installation {installation_id}")
