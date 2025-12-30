"""Base entity manager for Fusionbase SDK."""

import asyncio
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from fusionbase.entities.base import Entity
from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import ResourceNotFoundError

T = TypeVar('T', bound=Entity)

# Wherever you see _from_id, _afrom_id, or _http_client:
# pylint: disable=protected-access


class BaseEntityManager(Generic[T]):
    """Base manager for entity-specific operations.

    This class provides operations specific to a particular entity type.
    """

    def __init__(self, client, entity_class: Type[T]):
        """Initialize the entity type manager.

        Args:
            client: The Fusionbase client
            entity_class: The entity class this manager handles
        """
        self.client = client
        self.entity_class = entity_class

    def get(self, entity_id: str) -> T:
        """Get an entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity instance of type T

        Raises:
            ResourceNotFoundError: If entity not found
            APIError: If API request fails
        """
        # Protected access required to use entity-specific loading logic
        return self.entity_class._from_id(self.client, entity_id)

    def from_id(self, entity_id: str) -> T:
        """Get an entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity instance of type T

        Raises:
            ResourceNotFoundError: If entity not found
            APIError: If API request fails
        """
        return self.get(entity_id)

    async def aget(self, entity_id: str) -> T:
        """Asynchronously get an entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity instance

        Raises:
            ResourceNotFoundError: If entity not found
            APIError: If API request fails
        """
        if hasattr(self.entity_class, "_afrom_id"):
            return await self.entity_class._afrom_id(self.client, entity_id)
        # Create synchronous function in an async manner
        return await asyncio.to_thread(self.entity_class._from_id, self.client,
                                       entity_id)

    async def afrom_id(self, entity_id: str) -> T:
        """Asynchronously get an entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity instance

        Raises:
            ResourceNotFoundError: If entity not found
            APIError: If API request fails
        """
        result = await self.aget(entity_id)
        return result

    def _make_request(self, path: str, **kwargs) -> Dict[str, Any]:
        """Make an API request.

        Args:
            path: API path
            **kwargs: Additional request parameters

        Returns:
            API response data

        Raises:
            APIError: If request fails
        """
        try:
            # Protected access needed to use client's HTTP client directly
            if "GET" in kwargs:
                response = self.client._http_client.get(path, **kwargs)
            else:
                response = self.client._http_client.request(path, **kwargs)

            response.raise_for_status()
            return response.json()
        except (APIError, ResourceNotFoundError, AuthenticationError):
            # Re-raise specific exceptions
            raise
        except Exception as e:
            # Check if the exception has the response attribute before accessing it
            status_code = None
            if hasattr(e, 'response'):
                status_code = getattr(e.response, 'status_code', None)

            raise APIError(f"Error making request to {path}: {str(e)}",
                           status_code=status_code) from e

    def get_many(
        self,
        entity_ids: List[str],
        max_workers: int = 10,
        skip_errors: bool = False
    ) -> Union[List[T], Dict[str, Union[T, Exception]]]:
        """Get multiple entities by their IDs efficiently.

        Fetches multiple entities concurrently using a thread pool for better
        performance when retrieving many entities at once.

        Args:
            entity_ids: List of entity IDs to fetch
            max_workers: Maximum number of concurrent requests (default: 10)
            skip_errors: If True, returns a dict with results/errors.
                        If False (default), raises on first error.

        Returns:
            If skip_errors=False: List of entities in the same order as entity_ids
            If skip_errors=True: Dict mapping entity_id to entity or exception

        Raises:
            ResourceNotFoundError: If any entity not found (when skip_errors=False)
            APIError: If any API request fails (when skip_errors=False)

        Example:
            ```python
            # Fetch multiple organizations
            org_ids = ["id1", "id2", "id3"]
            orgs = client.entities.organizations.get_many(org_ids)

            # Fetch with error handling
            results = client.entities.organizations.get_many(
                org_ids,
                skip_errors=True
            )
            for entity_id, result in results.items():
                if isinstance(result, Exception):
                    print(f"Failed to fetch {entity_id}: {result}")
                else:
                    print(f"Fetched: {result.name}")
            ```
        """
        if not entity_ids:
            return {} if skip_errors else []

        # Remove duplicates while preserving order
        unique_ids = list(dict.fromkeys(entity_ids))

        results: Dict[str, Union[T, Exception]] = {}

        def fetch_entity(entity_id: str) -> tuple:
            """Fetch a single entity and return (id, result_or_error)."""
            try:
                entity = self.get(entity_id)
                return (entity_id, entity)
            except Exception as e:
                return (entity_id, e)

        # Use ThreadPoolExecutor for concurrent fetching
        # Limit workers to the number of IDs to avoid unnecessary threads
        actual_workers = min(max_workers, len(unique_ids))
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Submit all fetch tasks
            futures = {
                executor.submit(fetch_entity, eid): eid for eid in unique_ids
            }

            # Collect results as they complete
            for future in as_completed(futures):
                entity_id, result = future.result()
                results[entity_id] = result

        # Handle errors based on skip_errors flag
        if not skip_errors:
            # Check for any errors and raise the first one found
            for entity_id in unique_ids:
                result = results[entity_id]
                if isinstance(result, Exception):
                    raise result

            # Return list in original order
            return [results[eid] for eid in entity_ids]

        return results

    async def aget_many(
        self,
        entity_ids: List[str],
        max_concurrent: int = 10,
        skip_errors: bool = False
    ) -> Union[List[T], Dict[str, Union[T, Exception]]]:
        """Asynchronously get multiple entities by their IDs.

        Fetches multiple entities concurrently using asyncio for better
        performance when retrieving many entities at once.

        Args:
            entity_ids: List of entity IDs to fetch
            max_concurrent: Maximum number of concurrent requests (default: 10)
            skip_errors: If True, returns a dict with results/errors.
                        If False (default), raises on first error.

        Returns:
            If skip_errors=False: List of entities in the same order as entity_ids
            If skip_errors=True: Dict mapping entity_id to entity or exception

        Raises:
            ResourceNotFoundError: If any entity not found (when skip_errors=False)
            APIError: If any API request fails (when skip_errors=False)

        Example:
            ```python
            # Fetch multiple persons asynchronously
            person_ids = ["id1", "id2", "id3"]
            persons = await client.entities.persons.aget_many(person_ids)

            # Fetch with error handling
            results = await client.entities.persons.aget_many(
                person_ids,
                skip_errors=True
            )
            for entity_id, result in results.items():
                if isinstance(result, Exception):
                    print(f"Failed to fetch {entity_id}: {result}")
                else:
                    print(f"Fetched: {result.name}")
            ```
        """
        if not entity_ids:
            return {} if skip_errors else []

        # Remove duplicates while preserving order
        unique_ids = list(dict.fromkeys(entity_ids))

        results: Dict[str, Union[T, Exception]] = {}

        # Use semaphore to limit concurrency
        # Limit to the number of IDs to avoid unnecessary overhead
        actual_concurrent = min(max_concurrent, len(unique_ids))
        semaphore = asyncio.Semaphore(actual_concurrent)

        async def fetch_entity(entity_id: str) -> tuple:
            """Fetch a single entity with semaphore control."""
            async with semaphore:
                try:
                    entity = await self.aget(entity_id)
                    return (entity_id, entity)
                except Exception as e:
                    return (entity_id, e)

        # Create tasks for all entities
        tasks = [fetch_entity(eid) for eid in unique_ids]

        # Execute all tasks concurrently
        completed = await asyncio.gather(*tasks)

        # Build results dict
        for entity_id, result in completed:
            results[entity_id] = result

        # Handle errors based on skip_errors flag
        if not skip_errors:
            # Check for any errors and raise the first one found
            for entity_id in unique_ids:
                result = results[entity_id]
                if isinstance(result, Exception):
                    raise result

            # Return list in original order
            return [results[eid] for eid in entity_ids]

        return results
