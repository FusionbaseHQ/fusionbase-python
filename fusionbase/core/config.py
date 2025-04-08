"""Configuration handling for Fusionbase SDK."""

import os
import tempfile
from typing import Any, Optional

from pydantic import BaseModel
from pydantic import Field


class RetryConfig(BaseModel):
    """Configuration for API request retries.

    Attributes:
        enabled: Whether to retry failed requests
        max_attempts: Maximum number of retry attempts
        min_wait_seconds: Minimum wait time between retries
        max_wait_seconds: Maximum wait time between retries
        retry_statuses: HTTP status codes to retry
        retry_exceptions: Exception types to retry
    """

    enabled: bool = True
    max_attempts: int = 5
    min_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    retry_statuses: list[int] = Field(
        default_factory=lambda: [429, 500, 502, 503, 504])
    retry_exceptions: list[str] = Field(
        default_factory=lambda:
        ["ConnectionError", "TimeoutError", "ReadTimeout"])


class CacheConfig(BaseModel):
    """Configuration for caching.

    Attributes:
        enabled: Whether caching is enabled
        ttl_seconds: Default time-to-live in seconds
        directory: Custom directory for cache files
        size_limit: Maximum cache size in bytes
    """

    enabled: bool = True
    ttl_seconds: int = 3600  # 1 hour
    directory: Optional[str] = Field(default_factory=lambda: os.path.join(
        tempfile.gettempdir(), "fusionbase-cache"))
    size_limit: int = 1_000_000_000  # ~1GB


class LoggingConfig(BaseModel):
    """Configuration for logging.

    Attributes:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log message format
        log_requests: Whether to log API requests
        log_responses: Whether to log API responses
        hide_sensitive_data: Whether to redact sensitive information
    """

    level: str = "WARNING"
    format: str = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
    log_requests: bool = False
    log_responses: bool = False
    hide_sensitive_data: bool = True


class FusionbaseConfig(BaseModel):
    """Configuration options for Fusionbase client.

    Attributes:
        timeout: HTTP timeout in seconds
        cache: Cache configuration
        retry: Retry configuration
        logging: Logging configuration
        async_mode: Whether to use async client by default
        max_connections: Maximum number of connections in the pool
    """

    timeout: float = 60.0
    cache: CacheConfig = Field(default_factory=CacheConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    async_mode: bool = False
    max_connections: int = 10

    def update(self, **kwargs: Any) -> None:
        """Update configuration with provided values.

        Args:
            **kwargs: Configuration options to update.
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                # Handle nested configs
                if key in ["cache", "retry", "logging"] and isinstance(
                        value, dict):
                    current = getattr(self, key)
                    for sub_key, sub_value in value.items():
                        if hasattr(current, sub_key):
                            setattr(current, sub_key, sub_value)
                else:
                    setattr(self, key, value)
