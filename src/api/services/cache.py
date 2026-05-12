"""
Simple in-memory cache with TTL (time-to-live).

Why caching matters:
- Snowflake queries take 1-3 seconds each
- Streamlit refreshes every 30s — without cache that's 30s × N queries
- Cache stores results for TTL seconds, returns instantly on repeat calls

How it works:
- Store: {key: (value, expiry_timestamp)}
- Get: if key exists AND not expired → return value, else return None
- This is NOT Redis — it's in-process memory
- Data is lost on API restart (fine for a portfolio project)
"""

import time
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class Cache:
    def __init__(self):
        # {key: (value, expiry_timestamp)}
        self._store: dict = {}

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Store value with expiry = now + ttl seconds."""
        expiry = time.time() + ttl
        self._store[key] = (value, expiry)
        logger.debug(f"Cache SET: {key} (ttl={ttl}s)")

    def get(self, key: str) -> Optional[Any]:
        """Return value if exists and not expired, else None."""
        if key not in self._store:
            return None

        value, expiry = self._store[key]

        if time.time() > expiry:
            # Expired — clean up and return None
            del self._store[key]
            logger.debug(f"Cache EXPIRED: {key}")
            return None

        logger.debug(f"Cache HIT: {key}")
        return value

    def delete(self, key: str) -> None:
        """Remove a specific key."""
        self._store.pop(key, None)

    def clear(self) -> None:
        """Clear entire cache — useful for testing."""
        self._store.clear()
        logger.info("Cache cleared")

    def size(self) -> int:
        """Number of non-expired keys currently cached."""
        now = time.time()
        return sum(1 for _, (_, exp) in self._store.items() if now <= exp)


# Single instance shared across all services
# This is the singleton pattern — one cache for the whole app
cache = Cache()