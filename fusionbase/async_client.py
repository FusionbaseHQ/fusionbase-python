"""Async client for interacting with the Fusionbase API."""

from typing import Any, Dict, Optional

import httpx
from loguru import logger
from tenacity import AsyncRetrying
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from fusionbase.config import FusionbaseConfig
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.search.manager import SearchManager


class AsyncFusionbaseClient:
    """Asynchronous client for interacting with the Fusionbase API.

    This class provides asynchronous methods to communicate with Fusionbase data.

    Attributes:
        api_key: The API key for authenticating with Fusionbase.
        base_url: The base URL of the Fusionbase API.
        config: Configuration options for the client.
        _async_http_client: The internal async HTTP client used for API communication.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        config: FusionbaseConfig,
        parent_client: Optional[Any] = None,
    ):
        """Initialize a new async Fusionbase client.

        Args:
            api_key: The API key for authenticating with Fusionbase.
            base_url: The base URL of the Fusionbase API.
            config: Configuration options for the client.
            parent_client: Parent sync client, if any
        """
        self.api_key = api_key
        self.base_url = base_url
        self.config = config
        self._parent_client = parent_client
        self._cache = parent_client._cache if parent_client else None

        # Configure HTTP client
        limits = httpx.Limits(
            max_connections=self.config.max_connections,
            max_keepalive_connections=self.config.max_connections,
        )

        self._async_http_client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._get_headers(),
            timeout=self.config.timeout,
            limits=limits,
        )

        # Create search manager for async client
        self.search = SearchManager(self)

        logger.debug(
            f"Initialized AsyncFusionbaseClient with base URL: {self.base_url}")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests including authentication.

        Returns:
            Dictionary of HTTP headers to use in requests.
        """
        version = "0.3.0"  # Default version
        if self._parent_client:
            version = self._parent_client.get_version()

        return {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"fusionbase-python-async/{version}",
        }

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Any:
        """Send an async HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data

        Raises:
            APIError: If the API returns an error
        """
        if not self.config.retry.enabled:
            # Direct request without retry
            return await self._perform_request(method, url, **kwargs)

        # Check cache first if enabled
        cache_key = f"{method}:{url}"
        if self._cache and self._cache.enabled:
            cached_result = self._cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {method} {url}")
                return cached_result

        # Setup retry parameters
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
                retry_if_exception_type(Exception),
        }

        # Try the request with retries
        try:
            async for attempt in AsyncRetrying(**retry_config):
                with attempt:
                    response = await self._perform_request(
                        method, url, **kwargs)
                    # Check if response code indicates retry is needed
                    if response.status_code in self.config.retry.retry_statuses:
                        raise RuntimeError(
                            f"Retrying for status code {response.status_code}")

                    # Process successful response
                    result = await self._process_response(response)

                    # Cache the result if enabled
                    if self._cache and self._cache.enabled:
                        self._cache.set(cache_key, result)

                    return result
        except Exception as e:  # pylint: disable=no-member, broad-except
            if hasattr(e, "response"):
                logger.debug(f"Request failed with response: {e.response}")
                await self._process_response(e.response)
            else:
                logger.debug(f"Request failed without response: {str(e)}")
                raise APIError(
                    f"Async request failed after {self.config.retry.max_attempts} attempts: {method} {url}",
                    500) from e

    async def _perform_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:  # pylint: disable=line-too-long
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

    async def _process_response(self, response: httpx.Response) -> Any:
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
        except httpx.HTTPStatusError as exc:
            raise parse_error_response(response) from exc
        except Exception as exc:  # pylint: disable=no-member, broad-except
            if hasattr(exc, "response"):
                logger.debug(
                    f"Failed to process response with response: {exc.response}")
                raise parse_error_response(exc.response) from exc

            logger.debug(
                f"Failed to process response without response: {str(exc)}")
            raise APIError(
                f"Failed to process async response: {exc}",
                500,
                str(response.content if hasattr(response, "content") else ""),
            ) from exc

    async def get(self, url: str, **kwargs: Any) -> Any:
        """Send an async GET request.

        Args:
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data
        """
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Any:
        """Send an async POST request.

        Args:
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data
        """
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> Any:
        """Send an async PUT request.

        Args:
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data
        """
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> Any:
        """Send an async DELETE request.

        Args:
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data
        """
        return await self.request("DELETE", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> Any:
        """Send an async PATCH request.

        Args:
            url: URL to request
            **kwargs: Additional arguments to pass to httpx

        Returns:
            API response data
        """
        return await self.request("PATCH", url, **kwargs)

    async def aclose(self):
        """Close the async HTTP client."""
        await self._async_http_client.aclose()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit. Closes connections."""
        await self.aclose()
