"""
Filesystem-based cache adapter using diskcache.

Provides local disk caching for session Q&A storage.
Does NOT support distributed locking (use RedisAdapter for that).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import diskcache_community as dc

from m_flow.adapters.cache.cache_db_interface import CacheDBInterface
from m_flow.adapters.exceptions.exceptions import (
    CacheConnectionError,
    SharedKuzuLockRequiresRedisError,
)
from m_flow.shared.files.storage.get_storage_config import get_storage_config
from m_flow.shared.logging_utils import get_logger

_log = get_logger("FSCacheAdapter")


class FSCacheAdapter(CacheDBInterface):
    """
    Local filesystem cache using diskcache.

    Suitable for single-node deployments. Does not support
    distributed locking (requires Redis).
    """

    def __init__(self) -> None:
        cfg = get_storage_config()
        root_dir = cfg["data_root_directory"]
        cache_path = os.path.join(root_dir, ".m_flow_fs_cache", "sessions_db")
        os.makedirs(cache_path, exist_ok=True)

        self.cache = dc.Cache(directory=cache_path, disk=dc.SafeDisk.with_options(on_error="miss"))
        self.cache.expire()

        _log.debug(f"FSCache initialized: {cache_path}")

    def acquire_lock(self):
        """Locking not supported - raises error."""
        raise SharedKuzuLockRequiresRedisError()

    def release_lock(self):
        """Locking not supported - raises error."""
        raise SharedKuzuLockRequiresRedisError()

    async def add_qa(
        self,
        user_id: str,
        session_id: str,
        question: str,
        context: str,
        answer: str,
        ttl: int | None = 86400,
    ) -> None:
        """Store Q&A entry in local cache."""
        try:
            key = f"agent_sessions:{user_id}:{session_id}"

            entry = {
                "time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                "question": question,
                "context": context,
                "answer": answer,
            }

            raw = self.cache.get(key)
            entries = json.loads(raw) if raw else []
            entries.append(entry)

            self.cache.set(key, json.dumps(entries, default=str), expire=ttl)

        except Exception as e:
            raise CacheConnectionError(f"FSCache add_qa failed: {e}") from e

    async def get_latest_qa(
        self,
        user_id: str,
        session_id: str,
        last_n: int = 5,
    ) -> list[dict] | None:
        """Retrieve recent Q&A entries."""
        key = f"agent_sessions:{user_id}:{session_id}"
        raw = self.cache.get(key)

        if not raw:
            return None

        entries = json.loads(raw)
        return entries[-last_n:] if len(entries) > last_n else entries

    async def get_all_qas(self, user_id: str, session_id: str) -> list[dict] | None:
        """Retrieve all session Q&A entries."""
        key = f"agent_sessions:{user_id}:{session_id}"
        raw = self.cache.get(key)

        return json.loads(raw) if raw else None

    async def close(self) -> None:
        """Cleanup cache resources."""
        if self.cache:
            self.cache.expire()
            self.cache.close()
