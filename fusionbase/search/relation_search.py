"""Relation search module."""

import inspect
import traceback
from typing import Optional

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.relation import Relation
from fusionbase.exceptions import APIError
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult


class RelationSearchParams(SearchParams):
    """Parameters for relation search.

    Attributes:
        q: Query string for searching relations (required)
        skip: Number of results to skip (for pagination)
        limit: Maximum number of results to return
    """

    q: str  # Make this required
    skip: int = 0
    limit: int = 10


class RelationSearch(BaseSearch[Relation]):
    """Search for relations.

    This class provides methods to search for relations in the Fusionbase platform.
    Search results contain LazyReference objects to Relation entities that are only
    loaded when accessed.
    """

    def __init__(self, client):
        """Initialize a relation search instance.

        Args:
            client: Fusionbase client
        """
        super().__init__(client, Relation)

    def search(self,
               params: Optional[RelationSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Relation]]:
        """Search for relations.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results with LazyReference instances to Relation entities

        Raises:
            APIError: If the search operation fails
            ValueError: If no query is provided
        """
        # Handle kwargs to support calling search(q="network")
        if kwargs and not params:
            params = RelationSearchParams(**kwargs)

        if params is None or not params.q:
            raise ValueError("Search query (q) is required for relation search")

        # Create query parameters dictionary
        query_params = params.model_dump(exclude_none=True)

        try:
            response_data = self.client.request("GET",
                                                "search/relation",
                                                params=query_params)

            # Process results
            relations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "id" not in entity_data and "key" not in entity_data:
                    continue

                # Use id if available, otherwise use key
                entity_id = entity_data.get("id", entity_data.get("key"))

                # Create a LazyReference to the Relation
                relation_ref = LazyReference(entity_id, Relation, self.client)
                relations.append(relation_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=relations,
                total=len(relations),  # API might not return total
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            raise APIError(f"Relation search failed: {str(e)}",
                           status_code=500) from e

    async def _fetch_async_data(self, params, query_params):
        """Helper to reduce branches for asearch."""
        # Use appropriate async client method
        if hasattr(self.client, "arequest"):
            return await self.client.arequest("GET",
                                              "search/relation",
                                              params=query_params)
        if hasattr(self.client, "aget"):
            return await self.client.aget("search/relation",
                                          params=query_params)
        if hasattr(self.client, "amake_request"):
            return await self.client.amake_request("search/relation",
                                                   params=query_params)
        if (hasattr(self.client, "request") and
                inspect.iscoroutinefunction(self.client.request)):
            return await self.client.request("GET",
                                             "search/relation",
                                             params=query_params)
        print("WARNING: No async methods found, falling back to sync")
        return self.search(params)

    async def asearch(self,
                      params: Optional[RelationSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Relation]]:
        """Search for relations asynchronously.

        Args:
            params: Search parameters
            **kwargs: Additional parameters that will be added to params

        Returns:
            Search results with LazyReference instances to Relation entities

        Raises:
            APIError: If the search operation fails
        """
        # Handle kwargs to support calling asearch(q="network")
        if kwargs and not params:
            params = RelationSearchParams(**kwargs)

        if params is None or not params.q:
            raise ValueError("Search query (q) is required for relation search")

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
            relations = []
            for result in response_data.get("results", []):
                if not isinstance(result, dict) or "entity" not in result:
                    continue

                entity_data = result.get("entity", {})
                if "id" not in entity_data and "key" not in entity_data:
                    continue

                # Use id if available, otherwise use key
                entity_id = entity_data.get("id", entity_data.get("key"))

                # Create a LazyReference to the Relation
                relation_ref = LazyReference(entity_id, Relation, self.client)
                relations.append(relation_ref)

            # Properly create SearchResult with the expected parameters
            return SearchResult(
                items=relations,
                total=len(relations),  # API might not return total
                limit=params.limit,
                skip=params.skip,
                params=params,
            )

        except (OSError, IOError) as e:
            print(f"Async search error: {type(e).__name__}: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise APIError("Async relation search failed",
                           status_code=500) from e
