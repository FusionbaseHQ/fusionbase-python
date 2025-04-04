"""Cache module for Fusionbase SDK."""

import functools
import hashlib
import json
import os
from typing import Any, Callable, cast, Optional, TypeVar

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

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            cache = getattr(self, cache_attr, None)
            if cache is None or not getattr(cache, "enabled", True):
                return func(self, *args, **kwargs)

            # Generate cache key
            if key_builder:
                key = key_builder(self, *args, **kwargs)
                if isinstance(key, (list, tuple, dict)):
                    key = json.dumps(key, sort_keys=True)
                cache_key = f"{func.__module__}.{func.__name__}:{key}"
            else:
                key = generate_cache_key(args, kwargs)
                cache_key = f"{func.__module__}.{func.__name__}:{key}"

            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result

            # Call the function and cache the result
            result = func(self, *args, **kwargs)
            if result is not None:
                cache.set(cache_key, result, ttl=ttl)

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
            size_limit: int = 1_000_000_000  # Default ~1GB
    ):
        """Initialize the Fusionbase cache.

        Args:
            cache_dir: Directory to store the cache, defaults to user cache dir
            ttl: Time-to-live for cache entries in seconds, defaults to 1 hour
            enabled: Whether caching is enabled
            size_limit: Maximum size of the cache in bytes
        """
        self.cache_dir = cache_dir or get_default_cache_dir()
        self.ttl = ttl
        self.enabled = enabled
        self.size_limit = size_limit
        self._cache = None
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
        if not self._cache:
            return None

        try:
            value = self._cache.get(key)
            if value:
                logger.debug(f"Cache hit: {key}")
            else:
                logger.debug(f"Cache miss: {key}")
            return value
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error retrieving from cache: {str(e)}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds, uses instance default if not specified

        Returns:
            True if successful, False otherwise
        """
        if not self._cache:
            return False

        ttl = ttl if ttl is not None else self.ttl

        try:
            self._cache.set(key, value, expire=ttl)
            logger.debug(f"Cached: {key} (TTL: {ttl}s)")
            return True
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error writing to cache: {str(e)}")
            return False

    def invalidate(self, key_prefix: str = "") -> int:
        """Invalidate cache entries with a given key prefix.

        Args:
            key_prefix: Prefix of cache keys to invalidate

        Returns:
            Number of invalidated entries
        """
        if not self._cache:
            return 0

        cache_instance = self._cache
        count = 0
        try:
            for key in list(cache_instance):
                if str(key).startswith(key_prefix):
                    del cache_instance[key]
                    count += 1
            logger.debug(
                f"Invalidated {count} cache entries with prefix '{key_prefix}'")
            return count
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error invalidating cache: {str(e)}")
            return 0

    def clear(self) -> bool:
        """Clear the entire cache.

        Returns:
            True if successful, False otherwise
        """
        if not self._cache:
            return False

        try:
            self._cache.clear()
            logger.debug("Cache cleared")
            return True
        except (diskcache.Timeout, IOError, OSError) as e:
            logger.warning(f"Error clearing cache: {str(e)}")
            return False

    def close(self) -> None:
        """Close the cache."""
        if self._cache:
            self._cache.close()
            logger.debug("Cache closed")
            self._cache = None
