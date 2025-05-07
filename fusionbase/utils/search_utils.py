"""Utility functions for search operations."""

import inspect
import json
import traceback
from typing import Any, Dict, Optional, Type, TypeVar

from fusionbase.entities.base import Entity
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.exceptions import APIError
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult

T = TypeVar('T', bound=Entity)


def prepare_search_params(params: Optional[SearchParams] = None,
                          param_class: Type[SearchParams] = SearchParams,
                          require_query: bool = True,
                          **kwargs) -> tuple[SearchParams, Dict[str, Any]]:
    """Prepare search parameters and convert to query dictionary.

    Args:
        params: The search parameters object, if provided
        param_class: The class to use for creating parameters from kwargs
        require_query: Whether to require a query parameter
        **kwargs: Additional keyword arguments to be used as parameters

    Returns:
        Tuple of (SearchParams, query_params_dict)

    Raises:
        ValueError: If query is required but not provided
    """
    # Handle kwargs to support calling search(q="term")
    if kwargs and not params:
        params = param_class(**kwargs)

    if params is None:
        params = param_class()

    # Check for query requirement
    if require_query and hasattr(params, 'q') and not getattr(params, 'q'):
        raise ValueError(
            f"Search query (q) is required for {param_class.__name__}")

    # Create query parameters dictionary
    query_params = params.model_dump(exclude_none=True)

    # Handle filters if present - convert dict to JSON string
    if "filters" in query_params and isinstance(query_params["filters"], dict):
        query_params["filters"] = json.dumps(query_params["filters"])

    return params, query_params


def process_search_results(
        response_data: Dict[str, Any],
        entity_class: Type[T],
        client: Any,
        params: SearchParams,
        entity_id_field: str = "fb_entity_id"
) -> SearchResult[LazyReference[T]]:
    """Process search results into a SearchResult object with LazyReferences.

    Args:
        response_data: The response data from the API
        entity_class: The entity class for the LazyReferences
        client: The client to use for LazyReferences
        params: The search parameters used
        entity_id_field: The field name for the entity ID

    Returns:
        A SearchResult containing LazyReference objects
    """
    # Ensure we have a 'results' key
    if "results" not in response_data:
        return SearchResult(
            items=[],
            total=0,
            limit=getattr(params, 'limit', 10),
            skip=getattr(params, 'skip', 0),
            params=params,
        )

    # Process results
    entities = []
    for result in response_data.get("results", []):
        if not isinstance(result, dict) or "entity" not in result:
            continue

        entity_data = result.get("entity", {})

        # For relations, handle the different ID field
        if entity_class.__name__ == "Relation":
            if "id" not in entity_data and "key" not in entity_data:
                continue
            # Use id if available, otherwise use key
            entity_id = entity_data.get("id", entity_data.get("key"))
        else:
            # For other entity types
            if entity_id_field not in entity_data:
                continue
            entity_id = entity_data[entity_id_field]

        # Create a LazyReference to the entity
        entity_ref = LazyReference(entity_id, entity_class, client)
        entities.append(entity_ref)

    # Properly create SearchResult with the expected parameters
    return SearchResult(
        items=entities,
        total=response_data.get("total", len(entities)),
        limit=getattr(params, 'limit', 10),
        skip=getattr(params, 'skip', 0),
        params=params,
    )


def make_search_request(client: Any, endpoint: str,
                        params: Dict[str, Any]) -> Dict[str, Any]:
    """Make a synchronous search request.

    Args:
        client: The client to use for the request
        endpoint: The search endpoint
        params: The query parameters

    Returns:
        The response data

    Raises:
        APIError: If the search fails
    """
    try:
        # Make request to search endpoint
        if hasattr(client, "request"):
            return client.request("GET", endpoint, params=params)
        else:
            # For clients that don't have request method
            response = client.http_client.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
    except (OSError, IOError) as e:
        raise APIError(f"Search failed for {endpoint}: {str(e)}",
                       status_code=500) from e


async def make_search_request_async(
        client: Any,
        endpoint: str,
        params: Dict[str, Any],
        fallback_sync_method=None) -> Dict[str, Any]:
    """Make an asynchronous search request.

    Args:
        client: The client to use for the request
        endpoint: The search endpoint
        params: The query parameters
        fallback_sync_method: Optional synchronous method to use as fallback

    Returns:
        The response data

    Raises:
        APIError: If the search fails
    """
    try:
        data = None

        # Try appropriate async client methods
        if hasattr(client, "arequest"):
            data = await client.arequest("GET", endpoint, params=params)
        elif hasattr(client, "aget"):
            data = await client.aget(endpoint, params=params)
        elif hasattr(client, "amake_request"):
            data = await client.amake_request(endpoint, params=params)
        elif hasattr(client, "_async_http_client"):
            response = await client._async_http_client.get(endpoint,
                                                           params=params)
            response.raise_for_status()
            data = response.json()
        elif hasattr(client, "request") and inspect.iscoroutinefunction(
                client.request):
            data = await client.request("GET", endpoint, params=params)
        elif fallback_sync_method:
            # Try to use the fallback sync method in a thread
            import asyncio
            print("WARNING: No async methods found, falling back to sync")
            return await asyncio.to_thread(fallback_sync_method)
        else:
            # No suitable async method found, raise an error
            raise APIError(
                f"No suitable async method found for search endpoint {endpoint}",
                status_code=500)

        # Convert response_data to dict if it's not already
        if data and not isinstance(data, dict):
            if hasattr(data, "json"):
                data = data.json()
            elif hasattr(data, "__dict__"):
                data = data.__dict__

        return data

    except (OSError, IOError) as e:
        print(f"Async search error: {type(e).__name__}: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        raise APIError(f"Async search failed for {endpoint}",
                       status_code=500) from e
