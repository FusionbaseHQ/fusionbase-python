"""Logging configuration for Fusionbase SDK."""

import re
import sys
import threading
from typing import Any, Dict, List, Optional, Union
import uuid

import httpx
from loguru import logger


class SensitiveDataFilter:
    """Filter to redact sensitive information in logs."""

    def __init__(self, patterns: Optional[List[str]] = None):
        """Initialize the filter with redaction patterns.

        Args:
            patterns: List of regex patterns for sensitive data
        """
        self.patterns = patterns or [
            r'(X-API-KEY|Authorization)\s*:\s*[\'"]?([^\'",\s]+)',
            r'"api[_-]?key"\s*:\s*"([^"]+)"',
            r'password\s*[:=]\s*[\'"]([^\'",\s]+)',
            r'token\s*[:=]\s*[\'"]([^\'",\s]+)',
        ]
        self.compiled_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.patterns
        ]

    def __call__(self, record: Dict[str, Any]) -> bool:
        """Process the log record to redact sensitive data.

        Args:
            record: Log record to filter

        Returns:
            Always True to allow the record
        """
        if isinstance(record["message"], str):
            for pattern in self.compiled_patterns:
                record["message"] = pattern.sub(r'\1: "[REDACTED]"',
                                                record["message"])

        # Also check 'extra' fields
        if "extra" in record and isinstance(record["extra"], dict):
            for key, value in record["extra"].items():
                if isinstance(value, str):
                    for pattern in self.compiled_patterns:
                        record["extra"][key] = pattern.sub(
                            r'\1: "[REDACTED]"', value)

        return True


class RequestIdFilter:
    """Add request ID to log records for traceability."""

    def __init__(self):
        """Initialize with thread-local storage for request IDs."""
        self._local = threading.local()

    def __call__(self, record: Dict[str, Any]) -> bool:
        """Process the log record to add request ID.

        Args:
            record: Log record to enhance

        Returns:
            Always True to allow the record
        """
        # Get or create a request ID for this thread using thread-local storage
        if not hasattr(self._local, 'request_id'):
            self._local.request_id = str(uuid.uuid4())

        record["extra"]["request_id"] = self._local.request_id
        return True


def configure_logging(
    level: str = "INFO",
    log_format: str = "{time} | {level: <8} | {extra[request_id]} | {message}",
    sink: Optional[Union[str, Any]] = sys.stderr,
    hide_sensitive_data: bool = True,
    log_requests: bool = False,
    log_responses: bool = False,
    **kwargs: Any,
) -> None:
    """Configure Fusionbase logging with loguru.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_format: Log message format string.
        sink: Log output destination (file path or stream).
        hide_sensitive_data: Whether to redact sensitive information.
        log_requests: Whether to log API requests.
        log_responses: Whether to log API responses.
        **kwargs: Additional loguru logger configuration options.
    """
    # Remove existing handlers
    logger.remove()

    # Create filters
    filters = []
    if hide_sensitive_data:
        filters.append(SensitiveDataFilter())

    filters.append(RequestIdFilter())

    # Configure logger
    config: Dict[str, Any] = {
        "handlers": [{
            "sink": sink,
            "level": level,
            "format": log_format,
            "filter": lambda record: all(f(record) for f in filters),
            **kwargs,
        }],
        "extra": {
            "request_id": "00000000"
        },
    }

    logger.configure(**config)

    # Set global log flags
    logger.level("DEBUG", color="<cyan>")
    logger.level("INFO", color="<green>")
    logger.level("WARNING", color="<yellow>")
    logger.level("ERROR", color="<red>")
    logger.level("CRITICAL", color="<RED><bold>")

    # Configure httpx logging if needed
    if log_requests or log_responses:
        _configure_httpx_logging(log_requests, log_responses)


def _configure_httpx_logging(log_requests: bool, log_responses: bool) -> None:
    """Configure httpx logging with specific levels.

    Args:
        log_requests: Whether to log requests
        log_responses: Whether to log responses
    """
    # Monkey patch httpx to log requests and/or responses
    if log_requests:
        original_send = httpx.Client.send

        def logging_send(self, request, **kwargs):
            logger.debug(f"Request: {request.method} {request.url}")
            if request.content:
                logger.debug(f"Request body: {request.content}")
            return original_send(self, request, **kwargs)

        httpx.Client.send = logging_send

    if log_responses:
        original_raise_for_status = httpx.Response.raise_for_status

        def logging_raise_for_status(self):
            logger.debug(f"Response: {self.status_code} {self.reason_phrase}")
            if self.content:
                logger.debug(f"Response body: {self.text[:500]}")
            return original_raise_for_status(self)

        httpx.Response.raise_for_status = logging_raise_for_status
