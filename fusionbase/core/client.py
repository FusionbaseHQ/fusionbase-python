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
import sys
from typing import Any, Dict, Optional, TypeVar

import httpx
from loguru import logger
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import retry_if_result
from tenacity import RetryError
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from fusionbase.core.cache import cached
from fusionbase.core.cache import FusionbaseCache
from fusionbase.core.config import FusionbaseConfig
from fusionbase.core.context import set_current_client
from fusionbase.core.context import set_current_entity_manager
from fusionbase.core.logging import configure_logging
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.managers.entity_manager import EntityManager
from fusionbase.managers.search_manager import SearchManager

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
        self.api_key = api_key or os.getenv(
            'FUSIONBASE_API_KEY_COM') or os.getenv("FUSIONBASE_API_KEY")
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

        # Configure async HTTP client
        limits_async = httpx.Limits(
            max_connections=self.config.max_connections,
            max_keepalive_connections=self.config.max_connections,
        )

        self._async_http_client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._get_headers(),
            timeout=self.config.timeout,
            limits=limits_async,
        )

        # Create and register entity manager
        self.entities = EntityManager(self)
        self._register_entity_types()

        # Create and register search manager
        self.search = SearchManager(self)

        # Lazy-initialized managers for data operations
        self._datastreams = None
        self._dataservices = None

        # Set the context variables for this instance
        self._client_token = None
        self._manager_token = None

        logger.debug(
            f"Initialized Fusionbase client with base URL: {self.base_url}")

    def _register_entity_types(self):
        """Register all entity types with the entity manager."""
        # Import here to avoid circular imports
        from fusionbase.entities import Event
        from fusionbase.entities import Location
        from fusionbase.entities import Organization
        from fusionbase.entities.person import Person
        from fusionbase.types.entities import EntityType

        self.entities.register_entity_class(EntityType.LOCATION.value, Location)
        self.entities.register_entity_class(EntityType.ORGANIZATION.value,
                                            Organization)
        self.entities.register_entity_class(EntityType.PERSON.value, Person)
        self.entities.register_entity_class(EntityType.EVENT.value, Event)

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests including authentication.

        Returns:
            Dictionary of HTTP headers to use in requests.
        """
        return {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent":
                f"fusionbase-python/{self.get_version()} "
                f"(Python {'.'.join(map(str, os.sys.version_info[:3]))})",
        }

    def get_version(self) -> str:
        """Get the version of the fusionbase package.

        Returns:
            Version string.
        """
        try:
            return importlib.metadata.version("fusionbase")
        except Exception:
            return "0.8.0"

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
        response_format: str = None,
        **kwargs: Any,
    ) -> Any:
        """Send an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL to request
            response_format: Expected response format (json, msgpack)
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
            AuthenticationError: If authentication fails
        """
        if not self.config.retry.enabled:
            # Direct request without retry
            response = self._perform_request(method, url, **kwargs)
            return self._process_response(response, response_format)

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
            return self._process_response(response, response_format)
        except RetryError as exc:
            # Get the result from the last attempt - result() is a method, not a property
            response = None
            if exc.last_attempt is not None:
                try:
                    response = exc.last_attempt.result()
                except Exception:
                    # If result() raises an exception, we don't have a valid response
                    pass

            if response is not None:
                # If we have a response, process it to get appropriate error
                self._process_response(
                    response, response_format
                )  # This will raise the appropriate exception

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

    def _process_response(self,
                          response: httpx.Response,
                          response_format: str = None) -> Any:
        """Process the HTTP response.

        Args:
            response: The HTTP response
            response_format: Expected response format (json, msgpack)

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
        """
        try:
            response.raise_for_status()

            # Check if response is expected to be in msgpack format
            if response_format == "msgpack" and 'msgpack' in sys.modules:
                import msgpack
                return msgpack.unpackb(response.content, raw=False)

            # Fall back to JSON
            return response.json()

        except httpx.HTTPStatusError:
            raise parse_error_response(response)
        except Exception as exc:
            # Use a different variable name to avoid overwriting the response parameter
            exc_response = getattr(exc, "response", None)
            if exc_response:
                raise parse_error_response(exc_response) from exc

            # Handle case where response might not be a proper response object
            content_str = ""
            if hasattr(response, "content"):
                # For binary content like msgpack, show hex representation
                if response_format == "msgpack":
                    content_str = f"binary data ({len(response.content)} bytes)"
                else:
                    content_str = str(response.content)
            elif hasattr(response, "__call__"):  # Check if it's a function
                content_str = f"[Function: {response.__name__ if hasattr(response, '__name__') else 'unknown'}]"
            else:
                content_str = str(response)

            raise APIError(
                f"Failed to process response: {exc}",
                500,
                content_str,
            ) from exc

    async def arequest(self,
                       method: str,
                       url: str,
                       response_format: str = None,
                       **kwargs: Any) -> Any:
        """Send an async HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL to request
            response_format: Expected response format (json, msgpack)
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
        """
        if not self.config.retry.enabled:
            # Direct request without retry
            response = await self._perform_async_request(method, url, **kwargs)
            return await self._process_async_response(response, response_format)

        # Setup retry parameters similar to synchronous request method
        retry_config = {
            "stop":
                stop_after_attempt(self.config.retry.max_attempts),
            "wait":
                wait_exponential(
                    multiplier=1,
                    min=self.config.retry.min_wait_seconds,
                    max=self.config.retry.max_wait_seconds,
                ),
            "retry":
                retry_if_exception_type(
                    tuple(exc for exc_name in self.config.retry.retry_exceptions
                          for exc in [globals().get(exc_name, Exception)]
                          if exc is not None)),
        }

        from tenacity import AsyncRetrying

        # Try the request with retries
        try:
            async for attempt in AsyncRetrying(**retry_config):
                with attempt:
                    response = await self._perform_async_request(
                        method, url, **kwargs)
                    return await self._process_async_response(
                        response, response_format)
        except Exception as e:
            if hasattr(e, "response"):
                logger.debug(f"Request failed with response: {e.response}")
                await self._process_async_response(e.response, response_format)

            # If no suitable error was raised by processing the response, raise a generic one
            logger.debug(f"Request failed without response: {str(e)}")
            raise APIError(
                f"Async request failed after {self.config.retry.max_attempts} attempts: {method} {url}",
                500) from e

    async def _perform_async_request(self, method: str, url: str,
                                     **kwargs: Any) -> httpx.Response:
        """Perform the actual async HTTP request.

        Args:
            method: HTTP method
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            HTTP response

        Raises:
            Exception: If the request fails
        """
        logger.debug(f"Sending async {method} request to {url}")
        if method.upper() == "GET":
            response = await self._async_http_client.get(url, **kwargs)
        elif method.upper() == "POST":
            response = await self._async_http_client.post(url, **kwargs)
        elif method.upper() == "PUT":
            response = await self._async_http_client.put(url, **kwargs)
        elif method.upper() == "DELETE":
            response = await self._async_http_client.delete(url, **kwargs)
        elif method.upper() == "PATCH":
            response = await self._async_http_client.patch(url, **kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        logger.debug(f"Received async response: {response.status_code}")
        return response

    async def _process_async_response(self,
                                      response: httpx.Response,
                                      response_format: str = None) -> Any:
        """Process the HTTP response asynchronously.

        Args:
            response: The HTTP response
            response_format: Expected response format (json, msgpack)

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
        """
        try:
            response.raise_for_status()

            # Check if response is expected to be in msgpack format
            if response_format == "msgpack" and 'msgpack' in sys.modules:
                import msgpack
                return msgpack.unpackb(response.content, raw=False)
            else:
                # Fall back to JSON
                return response.json()

        except httpx.HTTPStatusError:
            raise parse_error_response(response)
        except Exception as exc:
            # Use a different variable name to avoid overwriting the response parameter
            exc_response = getattr(exc, "response", None)
            if exc_response:
                raise parse_error_response(exc_response) from exc

            # Handle case where response might not be a proper response object
            content_str = ""
            if hasattr(response, "content"):
                # For binary content like msgpack, show hex representation
                if response_format == "msgpack":
                    content_str = f"binary data ({len(response.content)} bytes)"
                else:
                    content_str = str(response.content)
            elif hasattr(response, "__call__"):  # Check if it's a function
                content_str = f"[Function: {response.__name__ if hasattr(response, '__name__') else 'unknown'}]"
            else:
                content_str = str(response)

            raise APIError(
                f"Failed to process async response: {exc}",
                500,
                content_str,
            ) from exc

    async def aget(self, url: str, **kwargs: Any) -> Any:
        """Send an async GET request."""
        return await self.arequest("GET", url, **kwargs)

    async def apost(self, url: str, **kwargs: Any) -> Any:
        """Send an async POST request."""
        return await self.arequest("POST", url, **kwargs)

    async def aput(self, url: str, **kwargs: Any) -> Any:
        """Send an async PUT request."""
        return await self.arequest("PUT", url, **kwargs)

    async def adelete(self, url: str, **kwargs: Any) -> Any:
        """Send an async DELETE request."""
        return await self.arequest("DELETE", url, **kwargs)

    async def apatch(self, url: str, **kwargs: Any) -> Any:
        """Send an async PATCH request."""
        return await self.arequest("PATCH", url, **kwargs)

    async def __aenter__(self):
        """Async context manager entry."""
        self._client_token = set_current_client(self)
        self._manager_token = set_current_entity_manager(self.entities)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self._client_token = None
        self._manager_token = None
        await self.aclose()

    async def aclose(self):
        """Close the HTTP client asynchronously."""
        if self._async_http_client:
            await self._async_http_client.aclose()

    def close(self):
        """Close the HTTP client and cache."""
        self._http_client.close()
        if self._cache:
            self._cache.close()

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

    @property
    def datastreams(self):
        """Get the DataStream manager for accessing data streams.

        Returns:
            DataStreamManager instance for stream operations.

        Example:
            >>> stream = client.datastreams.from_id("my_stream_id")
            >>> data = stream.get_data()
        """
        if self._datastreams is None:
            from fusionbase.managers.datastream_manager import DataStreamManager
            self._datastreams = DataStreamManager(self)
        return self._datastreams

    @property
    def dataservices(self):
        """Get the DataService manager for accessing data services.

        Returns:
            DataServiceManager instance for service operations.

        Example:
            >>> service = client.dataservices.from_id("my_service_id")
            >>> result = service.invoke({"param": "value"})
        """
        if self._dataservices is None:
            from fusionbase.managers.dataservice_manager import DataServiceManager
            self._dataservices = DataServiceManager(self)
        return self._dataservices

    def get_datastream(self, stream_id: str, validate: bool = True):
        """Get a DataStream by ID.

        Args:
            stream_id: The ID of the data stream to retrieve.
            validate: Whether to validate the stream exists (default: True).

        Returns:
            DataStream instance.

        Raises:
            ResourceNotFoundError: If validate=True and the stream doesn't exist.

        Example:
            >>> stream = client.get_datastream("my_stream_id")
            >>> for row in stream.get_data():
            ...     print(row)
        """
        return self.datastreams.from_id(stream_id, validate=validate)

    async def aget_datastream(self, stream_id: str, validate: bool = True):
        """Asynchronously get a DataStream by ID.

        Args:
            stream_id: The ID of the data stream to retrieve.
            validate: Whether to validate the stream exists (default: True).

        Returns:
            DataStream instance.

        Raises:
            ResourceNotFoundError: If validate=True and the stream doesn't exist.
        """
        return await self.datastreams.afrom_id(stream_id, validate=validate)

    def get_dataservice(self, service_id: str, validate: bool = True):
        """Get a DataService by ID.

        Args:
            service_id: The ID of the data service to retrieve.
            validate: Whether to validate the service exists (default: True).

        Returns:
            DataService instance.

        Raises:
            ResourceNotFoundError: If validate=True and the service doesn't exist.

        Example:
            >>> service = client.get_dataservice("my_service_id")
            >>> result = service.invoke({"input": "value"})
        """
        return self.dataservices.from_id(service_id, validate=validate)

    async def aget_dataservice(self, service_id: str, validate: bool = True):
        """Asynchronously get a DataService by ID.

        Args:
            service_id: The ID of the data service to retrieve.
            validate: Whether to validate the service exists (default: True).

        Returns:
            DataService instance.

        Raises:
            ResourceNotFoundError: If validate=True and the service doesn't exist.
        """
        return await self.dataservices.afrom_id(service_id, validate=validate)
