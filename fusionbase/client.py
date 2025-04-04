"""Main client for interacting with the Fusionbase API."""

# pylint: disable=too-many-instance-attributes
# pylint: disable=broad-exception-caught

# For lines longer than 88 chars:
# pylint: disable=C0301 # For line-too-long

# For import-outside-toplevel:
# pylint: disable=import-outside-toplevel

# For W0707 (raise-missing-from):
# pylint: disable=raise-missing-from

import importlib.metadata
import os
from typing import Any, Dict, Optional, TypeVar

import httpx
from loguru import logger
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import retry_if_result
from tenacity import RetryError
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from fusionbase.async_client import AsyncFusionbaseClient
from fusionbase.cache import cached
from fusionbase.cache import FusionbaseCache
from fusionbase.config import FusionbaseConfig
from fusionbase.context import set_current_client
from fusionbase.context import set_current_entity_manager
from fusionbase.entities.manager import EntityManager
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.logging import configure_logging
from fusionbase.search.manager import SearchManager

T = TypeVar('T')


class Fusionbase:
    """Main client for interacting with the Fusionbase API.

    This class provides methods to authenticate and communicate with Fusionbase data.
    It handles authentication and configuration, providing a foundation for
    data interaction methods.

    Attributes:
        api_key: The API key for authenticating with Fusionbase.
        base_url: The base URL of the Fusionbase API.
        config: Configuration options for the client.
        entities: Entity manager providing access to all entity types.
        search: Search manager providing access to search functionality.
        _http_client: The internal HTTP client used for API communication.
        _cache: Cache instance for storing responses.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.fusionbase.com/api/v2/",
        config: Optional[FusionbaseConfig] = None,
    ):
        """Initialize a new Fusionbase client.

        Args:
            api_key: The API key for authenticating with Fusionbase.
                    If not provided, will look for FUSIONBASE_API_KEY in environment.
            base_url: The base URL of the Fusionbase API.
            config: Configuration options for the client.

        Raises:
            ValueError: If no API key is available either as parameter or in environment.
        """
        self.api_key = api_key or os.getenv("FUSIONBASE_API_KEY")
        if not self.api_key:
            raise ValueError(
                "No API key provided. Pass it explicitly or set FUSIONBASE_API_KEY "
                "environment variable.")

        self.base_url = base_url
        self.config = config or FusionbaseConfig()

        # Configure logging based on client config
        configure_logging(
            level=self.config.logging.level,
            format=self.config.logging.format,
            hide_sensitive_data=self.config.logging.hide_sensitive_data,
            log_requests=self.config.logging.log_requests,
            log_responses=self.config.logging.log_responses,
        )

        # Initialize cache if enabled
        self._cache = None
        if self.config.cache.enabled:
            self._cache = FusionbaseCache(
                cache_dir=self.config.cache.directory,
                ttl=self.config.cache.ttl_seconds,
                enabled=self.config.cache.enabled,
                size_limit=self.config.cache.size_limit,
            )

        # Configure HTTP client
        limits = httpx.Limits(
            max_connections=self.config.max_connections,
            max_keepalive_connections=self.config.max_connections,
        )

        self._http_client = httpx.Client(
            base_url=self.base_url,
            headers=self._get_headers(),
            timeout=self.config.timeout,
            limits=limits,
        )

        # For async client
        self._async_http_client = None

        # Create and register entity manager
        self.entities = EntityManager(self)
        self._register_entity_types()

        # Create and register search manager
        self.search = SearchManager(self)

        # Set the context variables for this instance
        self._client_token = None
        self._manager_token = None

        logger.debug(
            f"Initialized Fusionbase client with base URL: {self.base_url}")

    def _register_entity_types(self):
        """Register all entity types with the entity manager."""
        # Import here to avoid circular imports
        from fusionbase.entities import Location
        from fusionbase.entities.types import EntityType

        self.entities.register_entity_class(EntityType.LOCATION.value, Location)

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests including authentication.

        Returns:
            Dictionary of HTTP headers to use in requests.
        """
        return {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"fusionbase-python/{self.get_version()}",
        }

    def get_version(self) -> str:
        """Get the version of the fusionbase package.

        Returns:
            Version string.
        """
        try:
            return importlib.metadata.version("fusionbase")
        except Exception:
            return "0.3.0"  # Fallback version

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if a request should be retried based on exception.

        Args:
            exception: The exception raised by the request

        Returns:
            True if the request should be retried, False otherwise
        """
        if not self.config.retry.enabled:
            return False

        # Check if exception type matches retry exceptions
        exception_name = exception.__class__.__name__
        return exception_name in self.config.retry.retry_exceptions

    def _should_retry_response(self, response: httpx.Response) -> bool:
        """Determine if a request should be retried based on response.

        Args:
            response: The HTTP response

        Returns:
            True if the request should be retried, False otherwise
        """
        if not self.config.retry.enabled:
            return False

        return response.status_code in self.config.retry.retry_statuses

    @cached(key_builder=lambda self, method, url, **kwargs:
            (f"{method}:{url}:" + f"{kwargs.get('params', '')}:" +
             f"{kwargs.get('json', '')}:" + f"{kwargs.get('data', '')}"))
    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Any:
        """Send an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
            AuthenticationError: If authentication fails
        """
        if not self.config.retry.enabled:
            # Direct request without retry
            return self._perform_request(method, url, **kwargs)

        # Create retry decorator with configured parameters
        retry_decorator = retry(
            stop=stop_after_attempt(self.config.retry.max_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=self.config.retry.min_wait_seconds,
                max=self.config.retry.max_wait_seconds,
            ),
            retry=(retry_if_exception_type(
                tuple(exc for exc_name in self.config.retry.retry_exceptions
                      for exc in [globals().get(exc_name, Exception)]
                      if exc is not None)) |
                   retry_if_result(lambda r: r.status_code in self.config.retry.
                                   retry_statuses
                                   if hasattr(r, "status_code") else False)),
            reraise=True,
        )

        # Apply retry decorator to request function
        @retry_decorator
        def _retried_request():
            return self._perform_request(method, url, **kwargs)

        try:
            response = _retried_request()
            return self._process_response(response)
        except RetryError as exc:
            response = getattr(exc.last_attempt, "result", None)
            if response:
                # If we have a response, process it to get appropriate error
                self._process_response(
                    response)  # This will raise the appropriate exception

            # If we don't have a response, raise generic error
            raise APIError(
                f"Request failed after {self.config.retry.max_attempts} attempts: {method} {url}",
                500,
            ) from exc

    def _perform_request(self, method: str, url: str,
                         **kwargs: Any) -> httpx.Response:
        """Perform the actual HTTP request.

        Args:
            method: HTTP method
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            HTTP response

        Raises:
            Exception: If the request fails
        """
        logger.debug(f"Sending {method} request to {url}")

        if method.upper() == "GET":
            response = self._http_client.get(url, **kwargs)
        elif method.upper() == "POST":
            response = self._http_client.post(url, **kwargs)
        elif method.upper() == "PUT":
            response = self._http_client.put(url, **kwargs)
        elif method.upper() == "DELETE":
            response = self._http_client.delete(url, **kwargs)
        elif method.upper() == "PATCH":
            response = self._http_client.patch(url, **kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        logger.debug(f"Received response: {response.status_code}")
        return response

    def _process_response(self, response: httpx.Response) -> Any:
        """Process the HTTP response.

        Args:
            response: The HTTP response

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
        """
        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            raise parse_error_response(response)
        except Exception as exc:
            response = getattr(exc, "response", None)
            if response:
                raise parse_error_response(response) from exc
            raise APIError(
                f"Failed to process response: {exc}",
                500,
                str(response.content if hasattr(response, "content") else ""),
            ) from exc

    async def aclose(self):
        """Close the HTTP client asynchronously."""
        if self._async_http_client:
            await self._async_http_client.aclose()

    def close(self):
        """Close the HTTP client and cache."""
        self._http_client.close()
        if self._cache:
            self._cache.close()

    @property
    def async_client(self) -> 'AsyncFusionbaseClient':
        """Get an async version of the client.

        Returns:
            An async client instance

        Note:
            This lazily initializes the async HTTP client
        """
        return AsyncFusionbaseClient(
            api_key=self.api_key,
            base_url=self.base_url,
            config=self.config,
            parent_client=self,
        )

    def __enter__(self):
        """Context manager entry. Sets this client as the current client."""
        self._client_token = set_current_client(self)
        self._manager_token = set_current_entity_manager(self.entities)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit. Resets the current client and closes connections."""
        self._client_token = None
        self._manager_token = None
        self.close()
