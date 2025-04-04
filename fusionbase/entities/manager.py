"""Entity managers for Fusionbase SDK."""

import asyncio
from typing import Any, Dict, Type, TypeVar

from fusionbase.entities.base import Entity
from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import ResourceNotFoundError

T = TypeVar('T', bound=Entity)

# Wherever you see _from_id, _afrom_id, or _http_client:
# pylint: disable=protected-access

# For import outside toplevel:
# pylint: disable=import-outside-toplevel


class BaseEntityManager:
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
            Entity instance

        Raises:
            ResourceNotFoundError: If entity not found
            APIError: If API request fails
        """
        # Protected access required to use entity-specific loading logic
        return self.entity_class._from_id(self.client, entity_id)

    # Add from_id method that tests expect
    def from_id(self, entity_id: str) -> T:
        """Get an entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity instance

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
        # Protected access required to use entity-specific async loading logic
        if hasattr(self.entity_class, "_afrom_id"):
            return await self.entity_class._afrom_id(self.client, entity_id)
        # Create synchronous function in an async manner
        return await asyncio.to_thread(self.entity_class._from_id, self.client,
                                       entity_id)

    # Add afrom_id method that tests expect
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


class EntityManager:
    """Manager class for entity operations.

    This class provides a central point for entity-related operations with
    specialized managers for each entity type.
    """

    def __init__(self, client):
        """Initialize the entity manager with a client.

        Args:
            client: A Fusionbase client instance
        """
        self.client = client
        self.entity_classes: Dict[str, Type[Entity]] = {}

        # Initialize type-specific managers lazily
        self._locations = None
        self._organizations = None
        self._persons = None
        self._events = None

    def register_entity_class(self, entity_type: str, cls: Type[T]) -> None:
        """Register an entity class with the manager.

        Args:
            entity_type: The type identifier for the entity
            cls: The entity class
        """
        self.entity_classes[entity_type] = cls

    @property
    def locations(self):
        """Get the locations manager.

        Returns:
            A manager for location entities
        """
        if self._locations is None:
            # Import here to avoid circular imports
            from fusionbase.entities.location import Location
            self._locations = BaseEntityManager(self.client, Location)
        return self._locations

    @property
    def persons(self):
        """Get the persons manager.

        Returns:
            A manager for person entities
        """
        if self._persons is None:
            # Import here to avoid circular imports
            from fusionbase.entities.person import Person
            self._persons = BaseEntityManager(self.client, Person)
        return self._persons
