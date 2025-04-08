"""Entity managers for Fusionbase SDK."""

from typing import Dict, Type, TypeVar

from fusionbase.entities.base import Entity

T = TypeVar('T', bound=Entity)

# For import outside toplevel:
# pylint: disable=import-outside-toplevel


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
        self._relations = None

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
            from fusionbase.managers.entity_managers import LocationManager
            self._locations = LocationManager(self.client, Location)
        return self._locations

    @property
    def organizations(self):
        """Get the organizations manager.

        Returns:
            A manager for organization entities
        """
        if self._organizations is None:
            # Import here to avoid circular imports
            from fusionbase.entities.organization import Organization
            from fusionbase.managers.entity_managers import OrganizationManager
            self._organizations = OrganizationManager(self.client, Organization)
        return self._organizations

    @property
    def persons(self):
        """Get the persons manager.

        Returns:
            A manager for person entities
        """
        if self._persons is None:
            # Import here to avoid circular imports
            from fusionbase.entities.person import Person
            from fusionbase.managers.entity_managers import PersonManager
            self._persons = PersonManager(self.client, Person)
        return self._persons

    @property
    def events(self):
        """Get the events manager.

        Returns:
            A manager for event entities
        """
        if self._events is None:
            # Import here to avoid circular imports
            from fusionbase.entities.event import Event
            from fusionbase.managers.entity_managers import EventManager
            self._events = EventManager(self.client, Event)
        return self._events

    @property
    def relations(self):
        """Get the relations manager.

        Returns:
            A manager for relation entities
        """
        if self._relations is None:
            # Import here to avoid circular imports
            from fusionbase.entities.relation import Relation
            from fusionbase.managers.entity_managers import RelationManager
            self._relations = RelationManager(self.client, Relation)
        return self._relations

    async def afrom_id(self, entity_type: str, entity_id: str) -> Entity:
        """Asynchronously get an entity by its type and ID.

        Args:
            entity_type: The type of entity to fetch
            entity_id: ID of the entity to fetch

        Returns:
            Entity instance

        Raises:
            ResourceNotFoundError: If entity not found
            APIError: If API request fails
        """
        entity_class = self.entity_classes.get(entity_type)
        if not entity_class:
            raise ValueError(f"Unknown entity type: {entity_type}")

        if hasattr(entity_class, "_afrom_id"):
            return await entity_class._afrom_id(self.client, entity_id)

        # Fallback to synchronous method via thread pool
        import asyncio
        return await asyncio.to_thread(entity_class._from_id, self.client,
                                       entity_id)

    async def aget(self, entity_type: str, entity_id: str) -> Entity:
        """Alias for afrom_id."""
        return await self.afrom_id(entity_type, entity_id)
