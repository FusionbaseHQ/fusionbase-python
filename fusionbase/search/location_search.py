"""Location search module."""

import inspect
import traceback
from typing import Optional

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.location import Location
from fusionbase.exceptions import APIError
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult


class LocationSearchParams(SearchParams):
    """Parameters for location search.

    Attributes:
        q: General search query (e.g., "Agnes-Pockels-Bogen 1, 80992 München")
        skip: Number of results to skip (for pagination) (default: 0)
        limit: Maximum number of results to return (default: 10)
    """

    q: str  # Make this required
    skip: int = 0  # Added skip parameter for consistency
    limit: int = 10


class LocationSearch(BaseSearch[Location]):
    """Search for locations.

    This class provides methods to search for locations in the Fusionbase platform.
    Search results contain LazyReference objects to Location entities that are only
    loaded when accessed.
    """

    def __init__(self, client):
        """Initialize a location search instance.

        Args:
            client: Fusionbase client
        """
        super().__init__(client, Location)

    def search(self,
               params: Optional[LocationSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Location]]:
        """Search for locations.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results with LazyReference instances to Location entities

        Raises:
            APIError: If the search operation fails
            ValueError: If no query is provided
        """
        # Handle kwargs to support calling search(q="address")
        if kwargs and not params:
            params = LocationSearchParams(**kwargs)

        if params is None or not params.q:
            raise ValueError("Search query (q) is required for location search")

        # Create query parameters dictionary
        query_params = params.model_dump(exclude_none=True)

        try:
            response_data = self.client.request("GET",
                                                "search/entities/location",
                                                params=query_params)

            # Process results
            locations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "fb_entity_id" not in entity_data:
                    continue

                # Create a LazyReference to the Location
                location_ref = LazyReference(entity_data["fb_entity_id"],
                                             Location, self.client)
                locations.append(location_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=locations,
                total=response_data.get("total", len(locations)),
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            raise APIError(f"Location search failed: {str(e)}",
                           status_code=500) from e

    async def _fetch_async_data(self, params, query_params):
        """Helper to reduce branches for asearch."""
        if hasattr(self.client, "async_client"):
            return await self.client.async_client.request(
                "GET", "search/entities/location", params=query_params)
        if hasattr(self.client, "amake_request"):
            return await self.client.amake_request("search/entities/location",
                                                   params=query_params)
        if (hasattr(self.client, "request") and
                inspect.iscoroutinefunction(self.client.request)):
            return await self.client.request("GET",
                                             "search/entities/location",
                                             params=query_params)
        print("WARNING: No async methods found, falling back to sync")
        return self.search(params)

    async def asearch(self,
                      params: Optional[LocationSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Location]]:
        """Search for locations asynchronously.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results with LazyReference instances to Location entities

        Raises:
            APIError: If the search operation fails
        """
        # Handle kwargs to support calling asearch(q="address")
        if kwargs and not params:
            params = LocationSearchParams(**kwargs)

        if params is None or not params.q:
            raise ValueError("Search query (q) is required for location search")

        # Create query parameters dictionary
        query_params = params.model_dump(exclude_none=True)

        try:
            response_data = await self._fetch_async_data(params, query_params)

            # Convert response_data to dict if it's not already
            if not isinstance(response_data, dict):
                if hasattr(response_data, "json"):
                    response_data = response_data.json()
                elif hasattr(response_data, "__dict__"):
                    response_data = response_data.__dict__

            # Ensure we have a 'results' key
            if "results" not in response_data:
                return SearchResult(
                    items=[],
                    total=0,
                    limit=params.limit,
                    skip=params.skip,
                    params=params,
                )

            # Process results
            locations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "fb_entity_id" not in entity_data:
                    continue

                # Create a LazyReference to the Location
                location_ref = LazyReference(entity_data["fb_entity_id"],
                                             Location, self.client)
                locations.append(location_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=locations,
                total=response_data.get("total", len(locations)),
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            print(f"Async search error: {type(e).__name__}: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise APIError("Async location search failed",
                           status_code=500) from e
