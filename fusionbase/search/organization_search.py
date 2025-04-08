"""Organization search module."""

import json
import traceback
from typing import Any, Dict, Optional

from pydantic import field_validator

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.organization import Organization
from fusionbase.exceptions import APIError
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.types.entities import FilterKey


class OrganizationSearchParams(SearchParams):
    """Parameters for organization search.

    Attributes:
        q: Query string for searching organizations (required)
        source_key: Data source identifier for filtering results
        skip: Number of results to skip (for pagination)
        limit: Maximum number of results to return
        filters: Additional filters as a dictionary that will be sent as JSON
                Valid filter keys are defined in the FilterKey enum:
                - FilterKey.ACTIVE: Filter by active status
                - FilterKey.STATUS: Filter by organization status
                - FilterKey.POSTAL_CODE: Filter by postal code
                - etc.
    """

    q: str  # Make this required
    source_key: Optional[str] = None
    skip: int = 0
    limit: int = 10
    filters: Optional[Dict[FilterKey, Any]] = None

    @field_validator('filters')
    @classmethod
    def validate_filter_keys(cls, v):
        """Ensure filter keys are all FilterKey enums."""
        if v is None:
            return v

        # Create a new dict with validated keys
        validated_filters = {}
        for key, value in v.items():
            # If key is already a FilterKey enum, use it directly
            if isinstance(key, FilterKey):
                validated_filters[key] = value
            else:
                # Attempt to convert string keys to FilterKey
                try:
                    enum_key = FilterKey(key)
                    validated_filters[enum_key] = value
                except ValueError:
                    raise ValueError(
                        f"Invalid filter key: '{key}'. Must be a FilterKey enum value. "
                        f"Valid values: {', '.join([f.value for f in FilterKey])}"
                    )

        return validated_filters


class OrganizationSearch(BaseSearch[Organization]):
    """Search for organizations.

    This class provides methods to search for organizations in the Fusionbase platform.
    Search results contain LazyReference objects to Organization entities that are only
    loaded when accessed.
    """

    def __init__(self, client):
        """Initialize a new organization search instance.

        Args:
            client: The Fusionbase client
        """
        super().__init__(client, Organization)

    def search(self,
               params: Optional[OrganizationSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Organization]]:
        """Search for organizations with given parameters.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results containing LazyReference instances to Organization entities

        Raises:
            APIError: If the search fails
            ValueError: If no query is provided
        """
        # Handle kwargs to support calling search(q="name")
        if kwargs and not params:
            params = OrganizationSearchParams(**kwargs)

        if params is None or not params.q:
            raise ValueError(
                "Search query (q) is required for organization search")

        # Create query parameters dictionary
        query_params = params.model_dump(exclude_none=True)

        # Handle filters - convert dict to JSON string if present
        if "filters" in query_params and isinstance(query_params["filters"],
                                                    dict):
            query_params["filters"] = json.dumps(query_params["filters"])

        try:
            # Make request to search endpoint
            if hasattr(self.client, "request"):
                response_data = self.client.request(
                    "GET", "search/entities/organization", params=query_params)
            else:
                # For clients that don't have request method
                response = self.client.http_client.get(
                    "search/entities/organization", params=query_params)
                response.raise_for_status()
                response_data = response.json()

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
            organizations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "fb_entity_id" not in entity_data:
                    continue

                # Create a LazyReference to the Organization
                org_ref = LazyReference(entity_data["fb_entity_id"],
                                        Organization, self.client)
                organizations.append(org_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=organizations,
                total=response_data.get("total", len(organizations)),
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            raise APIError(f"Organization search failed: {str(e)}",
                           status_code=500) from e

    async def _fetch_async_data(self, _, query_params):
        """Helper to reduce branches for asearch."""
        # Use appropriate async client method
        if hasattr(self.client, "arequest"):
            return await self.client.arequest("GET",
                                              "search/entities/organization",
                                              params=query_params)
        if hasattr(self.client, "aget"):
            return await self.client.aget("search/entities/organization",
                                          params=query_params)
        if hasattr(self.client,
                   "_async_http_client") and self.client._async_http_client:
            response = await self.client._async_http_client.get(
                "search/entities/organization", params=query_params)
            response.raise_for_status()
            return response.json()

        raise APIError("No suitable async method found for organization search",
                       status_code=500)

    async def asearch(self,
                      params: Optional[OrganizationSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Organization]]:
        """Search for organizations asynchronously with given parameters.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results containing LazyReference instances to Organization entities

        Raises:
            APIError: If the search fails
            ValueError: If no query is provided
        """
        # Handle kwargs to support calling asearch(q="name")
        if kwargs and not params:
            params = OrganizationSearchParams(**kwargs)

        if params is None or not params.q:
            raise ValueError(
                "Search query (q) is required for organization search")

        # Create query parameters dictionary
        query_params = params.model_dump(exclude_none=True)

        # Handle filters - convert dict to JSON string if present
        if "filters" in query_params and isinstance(query_params["filters"],
                                                    dict):
            query_params["filters"] = json.dumps(query_params["filters"])

        try:
            # Make async request to search endpoint
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

            # Process results - same logic as in sync version
            organizations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "fb_entity_id" not in entity_data:
                    continue

                # Create a LazyReference to the Organization
                org_ref = LazyReference(entity_data["fb_entity_id"],
                                        Organization, self.client)
                organizations.append(org_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=organizations,
                total=response_data.get("total", len(organizations)),
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            print(f"Async search error: {type(e).__name__}: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise APIError("Async organization search failed",
                           status_code=500) from e
