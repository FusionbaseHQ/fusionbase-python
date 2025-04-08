"""Cache module for Fusionbase SDK."""

import functools
from functools import lru_cache
import hashlib
import json
import os
import time
from typing import Any, Callable, cast, Dict, Optional, TypeVar

import diskcache
from loguru import logger
from platformdirs import user_cache_dir

# Type variable for generic function type
F = TypeVar('F', bound=Callable)


def get_default_cache_dir() -> str:
    """Get the default cache directory for Fusionbase SDK.

    Returns:
        The default cache directory path.
    """
    return os.path.join(user_cache_dir("fusionbase", "fusionbase"), "cache")


def generate_cache_key(args: tuple, kwargs: dict) -> str:
    """Generate a cache key from function arguments.

    Args:
        args: Positional arguments
        kwargs: Keyword arguments

    Returns:
        A unique cache key
    """
    # For simple and common cases, use direct string representation to avoid JSON overhead
    if not args and len(kwargs) == 1 and "entity_id" in kwargs:
        return f"id:{kwargs['entity_id']}"

    # Convert args and kwargs to a string representation
    args_str = json.dumps(args, sort_keys=True)
    kwargs_str = json.dumps(kwargs, sort_keys=True)

    # Generate a hash of the combined string
    key = hashlib.sha256((args_str + kwargs_str).encode()).hexdigest()
    return key


def cached(ttl: Optional[int] = None,
           key_builder: Optional[Callable] = None,
           cache_attr: str = "_cache") -> Callable[[F], F]:
    """Decorator to cache function results.

    Args:
        ttl: Time-to-live in seconds for cached results
        key_builder: Custom function to build cache keys
        cache_attr: Name of the cache attribute on the instance

    Returns:
        Decorated function with caching behavior
    """

    def decorator(func: F) -> F:
        # Create per-function in-memory LRU cache
        memory_cache = lru_cache(maxsize=128)(lambda k: (None, 0))

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            cache = getattr(self, cache_attr, None)
            if cache is None or not getattr(cache, "enabled", True):
                logger.debug(f"Cache disabled for {func.__name__}")
                return func(self, *args, **kwargs)

            # Generate cache key
            if key_builder:
                key = key_builder(self, *args, **kwargs)
                if isinstance(key, (list, tuple, dict)):
                    key = json.dumps(key, sort_keys=True)
            else:
                key = generate_cache_key(args, kwargs)

            cache_key = f"{func.__module__}.{func.__name__}:{key}"

            # First check in-memory cache
            try:
                mem_result, expire_time = memory_cache(cache_key)
                if mem_result is not None and (expire_time == 0 or
                                               expire_time > time.time()):
                    logger.debug(f"Memory cache hit: {func.__name__}")
                    return mem_result
            except Exception:
                # In case of any error with memory cache, just continue to disk cache
                pass

            # Try to get from disk cache
            start = time.time()
            cached_result = cache.get(cache_key)
            get_time = time.time() - start

            if cached_result is not None:
                # Store in memory cache for faster future access
                try:
                    effective_ttl = ttl if ttl is not None else getattr(
                        cache, 'ttl', 3600)
                    expire_time = time.time(
                    ) + effective_ttl if effective_ttl > 0 else 0
                    memory_cache.cache_clear()  # Avoid LRU cache filling up
                    memory_cache(cache_key)  # Populate in memory cache
                    memory_cache.__wrapped__.cache_update(
                        {cache_key: (cached_result, expire_time)})
                except Exception:
                    pass  # Continue even if in-memory caching fails

                if get_time > 0.01:  # Log if disk cache retrieval is slow
                    logger.info(
                        f"Disk cache hit for {func.__name__} (took {get_time:.3f}s)"
                    )
                return cached_result

            # Call the function and cache the result
            start = time.time()
            result = func(self, *args, **kwargs)
            exec_time = time.time() - start

            if result is not None:
                # Cache to disk
                cache.set(cache_key, result, ttl=ttl)

                # Cache to memory
                try:
                    effective_ttl = ttl if ttl is not None else getattr(
                        cache, 'ttl', 3600)
                    expire_time = time.time(
                    ) + effective_ttl if effective_ttl > 0 else 0
                    memory_cache.cache_clear()  # Avoid LRU cache filling up
                    memory_cache(cache_key)  # Populate cache
                    memory_cache.__wrapped__.cache_update(
                        {cache_key: (result, expire_time)})
                except Exception:
                    pass  # Continue even if in-memory caching fails

                logger.debug(
                    f"Cache miss for {func.__name__} (execution: {exec_time:.3f}s)"
                )

            return result

        return cast(F, wrapper)

    return decorator


class FusionbaseCache:
    """Cache implementation for Fusionbase SDK.

    Attributes:
        enabled: Whether caching is enabled
        size_limit: Maximum size of the cache in bytes
    """

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        ttl: int = 3600,
        enabled: bool = True,
        size_limit: int = 1_000_000_000,  # Default ~1GB
        memory_cache_size: int = 1000  # Store 1000 items in memory
    ):
        """Initialize the Fusionbase cache.

        Args:
            cache_dir: Directory to store the cache, defaults to user cache dir
            ttl: Time-to-live for cache entries in seconds, defaults to 1 hour
            enabled: Whether caching is enabled
            size_limit: Maximum size of the cache in bytes
            memory_cache_size: Maximum number of items in memory cache
        """
        self.cache_dir = cache_dir or get_default_cache_dir()
        self.ttl = ttl
        self.enabled = enabled
        self.size_limit = size_limit
        self._cache = None
        self._memory_cache: Dict[str, tuple] = {}  # key -> (value, expiry)
        self._memory_cache_size = memory_cache_size
        self._initialize_cache()

    def _initialize_cache(self) -> None:
        """Initialize the disk cache."""
        try:
            # Create cache directory if it doesn't exist
            os.makedirs(self.cache_dir, exist_ok=True)

            # Initialize disk cache
            self._cache = diskcache.Cache(self.cache_dir,
                                          size_limit=self.size_limit)
            logger.debug(f"Cache initialized at {self.cache_dir}")
        except (OSError, IOError) as e:
            logger.warning(f"Failed to initialize cache: {str(e)}")
            self._cache = None

    def get(self, key: str) -> Any:
        """Get a value from the cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        if not self.enabled:
            logger.debug(f"Cache disabled: miss for {key}")
            return None

        # First check memory cache
        if key in self._memory_cache:
            value, expiry = self._memory_cache[key]
            if expiry == 0 or expiry > time.time():
                logger.debug(f"Memory cache hit: {key}")
                return value
            # If expired, remove from memory cache
            del self._memory_cache[key]

        # If not in memory or expired, check disk cache
        if not self._cache:
            logger.debug(f"Disk cache not initialized: miss for {key}")
            return None

        try:
            value = self._cache.get(key)

            # If found, also add to memory cache for faster future access
            if value is not None:
                self._add_to_memory_cache(key, value)
                logger.debug(f"Disk cache hit: {key}")
            else:
                logger.debug(f"Cache miss: {key}")

            return value
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error retrieving from cache: {str(e)}")
            return None

    def _add_to_memory_cache(self,
                             key: str,
                             value: Any,
                             ttl: Optional[int] = None) -> None:
        """Add an item to the memory cache."""
        if len(self._memory_cache) >= self._memory_cache_size:
            # Simple LRU implementation: remove oldest key
            try:
                oldest_key = next(iter(self._memory_cache))
                del self._memory_cache[oldest_key]
            except (StopIteration, KeyError):
                pass

        # Calculate expiry time
        effective_ttl = ttl if ttl is not None else self.ttl
        expiry = time.time() + effective_ttl if effective_ttl > 0 else 0

        # Add to memory cache
        self._memory_cache[key] = (value, expiry)

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds, uses instance default if not specified

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            logger.debug(f"Cache disabled: skipping set for {key}")
            return False

        # Add to memory cache
        self._add_to_memory_cache(key, value, ttl)

        # Also add to disk cache
        if not self._cache:
            logger.debug(
                f"Disk cache not initialized: memory-only caching for {key}")
            return True

        effective_ttl = ttl if ttl is not None else self.ttl

        try:
            self._cache.set(key, value, expire=effective_ttl)
            logger.debug(f"Cached to disk: {key} (TTL: {effective_ttl}s)")
            return True
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error writing to disk cache: {str(e)}")
            return False

    def invalidate(self, key_prefix: str = "") -> int:
        """Invalidate cache entries with a given key prefix.

        Args:
            key_prefix: Prefix of cache keys to invalidate

        Returns:
            Number of invalidated entries
        """
        count = 0

        # Clear matching items from memory cache
        memory_keys_to_remove = [
            k for k in list(self._memory_cache.keys())
            if k.startswith(key_prefix)
        ]
        for k in memory_keys_to_remove:
            del self._memory_cache[k]
            count += 1

        # Clear from disk cache
        if self._cache:
            try:
                for key in list(self._cache.iterkeys()):
                    if str(key).startswith(key_prefix):
                        del self._cache[key]
                        count += 1

                logger.debug(
                    f"Invalidated {count} cache entries with prefix '{key_prefix}'"
                )
            except (diskcache.Timeout, IOError, OSError) as e:
                logger.warning(f"Error invalidating disk cache: {str(e)}")

        return count

    def clear(self) -> bool:
        """Clear the entire cache.

        Returns:
            True if successful, False otherwise
        """
        # Clear memory cache
        self._memory_cache.clear()
        logger.debug("Memory cache cleared")

        # Clear disk cache
        if not self._cache:
            return True  # Memory cache cleared, disk cache was not initialized

        try:
            self._cache.clear()
            logger.debug("Disk cache cleared")
            return True
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error clearing disk cache: {str(e)}")
            return False

    def close(self) -> None:
        """Close the cache."""
        if self._cache:
            try:
                self._cache.close()
                logger.debug("Cache closed")
            except Exception as e:
                logger.warning(f"Error closing cache: {str(e)}")
            self._cache = None
        self._memory_cache.clear()
