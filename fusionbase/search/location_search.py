"""Location search module."""

import inspect
import traceback
from typing import Optional

from fusionbase.entities.location import Location
from fusionbase.exceptions import APIError
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult


class LocationSearchParams(SearchParams):
    """Parameters for location search.

    Attributes:
        q: General search query (e.g., "Agnes-Pockels-Bogen 1, 80992 München")
        limit: Maximum number of results to return (default: 10)
    """

    q: str  # Make this required
    limit: int = 10


class LocationSearch(BaseSearch[Location]):
    """Search for locations.

    This class provides methods to search for locations in the Fusionbase platform.
    """

    def __init__(self, client):
        """Initialize a location search instance.

        Args:
            client: Fusionbase client
        """
        super().__init__(client, Location)

    def search(
        self,
        params: Optional[LocationSearchParams] = None
    ) -> SearchResult[Location]:
        """Search for locations.

        Args:
            params: Search parameters

        Returns:
            Search results

        Raises:
            APIError: If the search operation fails
            ValueError: If no query is provided
        """
        if params is None or not params.q:
            raise ValueError("Search query (q) is required for location search")

        # Create query parameters dictionary
        query_params = {}

        # Add parameters to the query
        query_params["q"] = params.q
        if params.limit:
            query_params["limit"] = str(params.limit)

        try:
            response_data = self.client.request("GET",
                                                "search/entities/location",
                                                params=query_params)

            # Process results
            locations = []
            for result in response_data.get("results", []):
                entity_data = result.get("entity", {})
                # Convert API response to Location object format
                location_data = {
                    "fb_entity_id":
                        entity_data.get("fb_entity_id"),
                    "fb_entity_version":
                        entity_data.get("fb_entity_version"),
                    "name":
                        entity_data.get("formatted_address"),
                    "metadata": {
                        "created_at": entity_data.get("created_at"),
                        "updated_at": entity_data.get("updated_at"),
                        "fb_datetime": entity_data.get("fb_datetime"),
                        "fb_semantic_id": entity_data.get("fb_semantic_id"),
                    },
                    "external_ids":
                        entity_data.get("external_ids", {}),
                    "coordinate":
                        entity_data.get("coordinate"),
                    "location_level":
                        entity_data.get("location_level"),
                    "address_components":
                        entity_data.get("address_components", []),
                    "alternative_names":
                        entity_data.get("alternative_names", []),
                    "fb_semantic_id":
                        entity_data.get("fb_semantic_id"),
                    "formatted_address":
                        entity_data.get("formatted_address"),
                    "entity_subtype":
                        entity_data.get("entity_subtype"),
                }
                locations.append(Location.model_validate(location_data))

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=locations,
                total=len(locations),
                limit=params.limit,
                offset=0,
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

    async def asearch(
        self,
        params: Optional[LocationSearchParams] = None
    ) -> SearchResult[Location]:
        """Search for locations asynchronously.

        Args:
            params: Search parameters

        Returns:
            Search results

        Raises:
            APIError: If the search operation fails
        """
        params = params or LocationSearchParams()

        # Create query parameters dictionary
        query_params = {}

        # Add parameters to the query
        if params.q:
            query_params["q"] = params.q
        if params.limit:
            query_params["limit"] = str(params.limit)

        try:
            response_data = await self._fetch_async_data(params, query_params)

            # Convert response_data to dict if it's not already
            if not isinstance(response_data, dict):
                print("WARNING: response_data not dict, converting...")
                if hasattr(response_data, "json"):
                    response_data = response_data.json()
                elif hasattr(response_data, "__dict__"):
                    response_data = response_data.__dict__

            # Ensure we have a 'results' key
            if "results" not in response_data:
                print(f"WARNING: 'results' not in response_data: "
                      f"{list(response_data.keys())}")
                return SearchResult(
                    items=[],
                    total=0,
                    limit=params.limit,
                    offset=0,
                    params=params,
                )

            # Process results - same logic as in sync version
            locations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                location_data = {
                    "fb_entity_id":
                        entity_data.get("fb_entity_id"),
                    "fb_entity_version":
                        entity_data.get("fb_entity_version"),
                    "name":
                        entity_data.get("formatted_address"),
                    "metadata": {
                        "created_at": entity_data.get("created_at"),
                        "updated_at": entity_data.get("updated_at"),
                        "fb_datetime": entity_data.get("fb_datetime"),
                        "fb_semantic_id": entity_data.get("fb_semantic_id"),
                    },
                    "external_ids":
                        entity_data.get("external_ids", {}),
                    "coordinate":
                        entity_data.get("coordinate"),
                    "location_level":
                        entity_data.get("location_level"),
                    "address_components":
                        entity_data.get("address_components", []),
                    "alternative_names":
                        entity_data.get("alternative_names", []),
                    "fb_semantic_id":
                        entity_data.get("fb_semantic_id"),
                    "formatted_address":
                        entity_data.get("formatted_address"),
                    "entity_subtype":
                        entity_data.get("entity_subtype"),
                }
                try:
                    locations.append(Location.model_validate(location_data))
                except ValueError as validation_error:
                    print(f"Failed to validate location: {validation_error}")
                    # Skip invalid locations

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=locations,
                total=len(locations),
                limit=params.limit,
                offset=0,
                params=params,
            )

        except (OSError, IOError) as e:
            print(f"Async search error: {type(e).__name__}: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise APIError("Async location search failed",
                           status_code=500) from e
