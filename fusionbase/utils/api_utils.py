"""Utility functions for API requests."""

import asyncio
import inspect
from typing import Any, Dict, Type, TypeVar

import httpx

from fusionbase.entities.base import Entity
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import ResourceNotFoundError

T = TypeVar('T', bound=Entity)


def make_entity_request(
        client: Any,
        entity_type: str,
        entity_id: str,
        endpoint_format: str = "entities/{}/get/{}") -> Dict[str, Any]:
    """Make an API request to fetch entity data.

    Args:
        client: API client to use for requests
        entity_type: Type of entity (e.g., "organization")
        entity_id: Entity ID to fetch
        endpoint_format: Format string for API endpoint

    Returns:
        Dictionary of entity data

    Raises:
        ResourceNotFoundError: If entity doesn't exist
        APIError: For other API errors
    """
    endpoint = endpoint_format.format(entity_type, entity_id)

    # Use the request method with retry if available
    if hasattr(client, "request"):
        try:
            return client.request("GET", endpoint)
        except ResourceNotFoundError as e:
            # Make the error more specific
            response = getattr(e, "response", None)
            raise ResourceNotFoundError(entity_type, entity_id, response) from e
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ResourceNotFoundError(entity_type, entity_id,
                                            e.response) from e
            raise parse_error_response(e.response) from e
    elif hasattr(client, "make_request"):
        # If client is an EntityManager
        try:
            return client.make_request(endpoint)
        except ResourceNotFoundError as e:
            response = getattr(e, "response", None)
            raise ResourceNotFoundError(entity_type, entity_id, response) from e
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ResourceNotFoundError(entity_type, entity_id,
                                            e.response) from e
            raise parse_error_response(e.response) from e
    else:
        try:
            response = client.http_client.get(endpoint)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ResourceNotFoundError(entity_type, entity_id,
                                            e.response) from e
            raise parse_error_response(e.response) from e
        except Exception as e:  # pylint: disable=broad-except
            if getattr(e, "response", None) is not None:
                raise parse_error_response(getattr(e, "response")) from e
            raise APIError(
                f"Failed to retrieve {entity_type} (ID: {entity_id}): {e}",
                500,
            ) from e


async def make_entity_request_async(
        client: Any,
        entity_type: str,
        entity_id: str,
        endpoint_format: str = "entities/{}/get/{}") -> Dict[str, Any]:
    """Make an async API request to fetch entity data.

    Args:
        client: API client to use for requests
        entity_type: Type of entity (e.g., "organization")
        entity_id: Entity ID to fetch
        endpoint_format: Format string for API endpoint

    Returns:
        Dictionary of entity data

    Raises:
        ResourceNotFoundError: If entity doesn't exist
        APIError: For other API errors
    """
    endpoint = endpoint_format.format(entity_type, entity_id)

    try:
        data = None
        # Try direct async methods on the client
        if hasattr(client, "aget"):
            data = await client.aget(endpoint)
        # Use arequest method if available
        elif hasattr(client, "arequest"):
            data = await client.arequest("GET", endpoint)
        # Use amake_request method if available (for entity managers)
        elif hasattr(client, "amake_request"):
            data = await client.amake_request(endpoint)
        # Use async HTTP client directly
        elif hasattr(client, "_async_http_client"):
            response = await client._async_http_client.get(endpoint)
            response.raise_for_status()
            data = response.json()
        else:
            # Fall back to sync method through asyncio.to_thread
            if hasattr(client, "request") and not inspect.iscoroutinefunction(
                    client.request):
                # Create a sync function that will be called in a thread
                return await asyncio.to_thread(make_entity_request, client,
                                               entity_type, entity_id,
                                               endpoint_format)

            # No suitable async method found, raise an error
            raise APIError(
                f"No suitable async method found to fetch {entity_type} (ID: {entity_id})",
                status_code=500)

        # Make sure we have data
        if not data:
            raise APIError(
                f"Failed to retrieve {entity_type} data (ID: {entity_id})",
                status_code=500)

        return data

    except ResourceNotFoundError as e:
        # Make the error more specific
        response = getattr(e, "response", None)
        raise ResourceNotFoundError(entity_type, entity_id, response) from e
    except httpx.HTTPStatusError as e:
        if getattr(e, "response", None) is not None:
            if getattr(e.response, "status_code", None) == 404:
                raise ResourceNotFoundError(entity_type, entity_id,
                                            e.response) from e
            raise parse_error_response(e.response) from e
        raise APIError(
            f"Failed to retrieve {entity_type} (ID: {entity_id}): {e}",
            500) from e


def fetch_entity_sync(cls: Type[T], client: Any, entity_id: str) -> T:
    """Fetch and create an entity instance synchronously.

    Args:
        cls: Entity class to instantiate
        client: API client
        entity_id: Entity ID to fetch

    Returns:
        Instantiated entity
    """
    # Get entity type from the class
    entity_type = cls.entity_type.value

    # Get endpoint format based on entity type - special case for relations
    endpoint_format = "relation/get/{}" if entity_type == "relation" else "entities/{}/get/{}"

    # Fetch the data
    data = make_entity_request(client, entity_type, entity_id, endpoint_format)

    # Create the entity instance - entity-specific preprocessing will be handled by each class
    entity = cls.model_validate(data)

    # Store client reference
    if hasattr(entity, "_client"):
        entity._client = client

    # Handle special post-processing for certain entity types
    if hasattr(entity, "_process_linked_entities"):
        entity._process_linked_entities(client)

    return entity


async def fetch_entity_async(cls: Type[T], client: Any, entity_id: str) -> T:
    """Fetch and create an entity instance asynchronously.

    Args:
        cls: Entity class to instantiate
        client: API client
        entity_id: Entity ID to fetch

    Returns:
        Instantiated entity
    """
    # Get entity type from the class
    entity_type = cls.entity_type.value

    # Get endpoint format based on entity type - special case for relations
    endpoint_format = "relation/get/{}" if entity_type == "relation" else "entities/{}/get/{}"

    # Fetch the data asynchronously
    data = await make_entity_request_async(client, entity_type, entity_id,
                                           endpoint_format)

    # Create the entity instance
    entity = cls.model_validate(data)

    # Store client reference
    if hasattr(entity, "_client"):
        entity._client = client

    # Handle special post-processing for certain entity types
    if hasattr(entity, "_process_linked_entities"):
        entity._process_linked_entities(client)

    return entity
