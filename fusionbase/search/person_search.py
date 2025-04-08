"""Person search module."""

import inspect
import traceback
from typing import Optional  # Removed unused imports: inspect, Any, Dict, List

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.person import Person
from fusionbase.exceptions import APIError
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult


class PersonSearchParams(SearchParams):
    """Parameters for person search.

    Attributes:
        q: Query string for searching persons
        source_key: Data source identifier for filtering results
        skip: Number of results to skip (for pagination)
        limit: Maximum number of results to return
    """

    q: Optional[str] = None
    source_key: Optional[str] = None
    skip: int = 0
    limit: int = 10


class PersonSearch(BaseSearch[Person]):
    """Search for persons.

    This class provides methods to search for persons in the Fusionbase platform.
    Search results contain LazyReference objects to Person entities that are only
    loaded when accessed.
    """

    def __init__(self, client):
        """Initialize a new person search instance.

        Args:
            client: The Fusionbase client
        """
        super().__init__(client, Person)

    def search(self,
               params: Optional[PersonSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Person]]:
        """Search for persons with given parameters.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results containing LazyReference instances to Person entities

        Raises:
            APIError: If the search fails
        """
        params = params or PersonSearchParams(**kwargs)
        query_params = params.model_dump(exclude_none=True)

        try:
            # Make request to search endpoint
            if hasattr(self.client, "request"):
                response_data = self.client.request("GET",
                                                    "search/entities/person",
                                                    params=query_params)
            else:
                # For clients that don't have request method
                response = self.client.http_client.get("search/entities/person",
                                                       params=query_params)
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
            persons = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "fb_entity_id" not in entity_data:
                    continue

                # Create a LazyReference to the Person
                person_ref = LazyReference(entity_data["fb_entity_id"], Person,
                                           self.client)
                persons.append(person_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=persons,
                total=response_data.get("total", len(persons)),
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            raise APIError(f"Person search failed: {str(e)}",
                           status_code=500) from e

    async def _fetch_async_data(self, params, query_params):
        """Helper to reduce branches for asearch."""
        # Use appropriate async client method
        if hasattr(self.client, "arequest"):
            return await self.client.arequest("GET",
                                              "search/entities/person",
                                              params=query_params)
        if hasattr(self.client, "aget"):
            return await self.client.aget("search/entities/person",
                                          params=query_params)
        if hasattr(self.client, "amake_request"):
            return await self.client.amake_request("search/entities/person",
                                                   params=query_params)
        if (hasattr(self.client, "request") and
                inspect.iscoroutinefunction(self.client.request)):
            return await self.client.request("GET",
                                             "search/entities/person",
                                             params=query_params)
        # Changed from elif to if as the previous condition has a return
        print("WARNING: No async methods found, falling back to sync")
        return self.search(params)

    async def asearch(self,
                      params: Optional[PersonSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Person]]:
        """Search for persons asynchronously with given parameters.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results containing LazyReference instances to Person entities

        Raises:
            APIError: If the search fails
        """
        params = params or PersonSearchParams(**kwargs)
        query_params = params.model_dump(exclude_none=True)

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
            persons = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "fb_entity_id" not in entity_data:
                    continue

                # Create a LazyReference to the Person
                person_ref = LazyReference(entity_data["fb_entity_id"], Person,
                                           self.client)
                persons.append(person_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=persons,
                total=response_data.get("total", len(persons)),
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            print(f"Async search error: {type(e).__name__}: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise APIError("Async person search failed", status_code=500) from e
