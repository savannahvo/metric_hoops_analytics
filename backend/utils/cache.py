"""
utils/cache.py
--------------
Thread-safe in-memory TTL cache decorator.
Prevents hammering external APIs on every request.
"""

import time
import threading
import functools
from typing import Any, Callable

# In-memory store: key -> (value, expires_at)
_cache: dict = {}
_lock = threading.Lock()


def cached(ttl_seconds: int = 60):
    """
    Decorator that caches the return value of a function for ttl_seconds.
    Cache key = function name + str(args) + str(kwargs).
    Thread-safe via threading.Lock().

    Usage:
        @cached(ttl_seconds=3600)
        def get_standings():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            key = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"

            with _lock:
                if key in _cache:
                    value, expires_at = _cache[key]
                    if time.time() < expires_at:
                        return value

            # Call outside the lock to avoid holding it during I/O
            result = func(*args, **kwargs)

            with _lock:
                _cache[key] = (result, time.time() + ttl_seconds)

            return result

        return wrapper
    return decorator


def clear_cache() -> None:
    """Flush all cache entries."""
    global _cache
    with _lock:
        _cache = {}


def cache_stats() -> dict:
    """Return cache statistics."""
    now = time.time()
    with _lock:
        total = len(_cache)
        active = sum(1 for _, (_, exp) in _cache.items() if exp > now)
    return {"total": total, "active": active, "expired": total - active}
