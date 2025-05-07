"""Specialized entity managers for better type hinting."""

from fusionbase.entities.event import Event
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person
from fusionbase.entities.relation import Relation
from fusionbase.managers.base_entity_manager import BaseEntityManager


class LocationManager(BaseEntityManager[Location]):
    """Manager for Location entities with improved type hinting."""

    def from_id(self, entity_id: str) -> Location:
        """Get a location by its ID. """
        return super().from_id(entity_id)

    async def afrom_id(self, entity_id: str) -> Location:
        """Get a location asynchronously by its ID. """
        return await super().afrom_id(entity_id)


class OrganizationManager(BaseEntityManager[Organization]):
    """Manager for Organization entities with improved type hinting."""

    def from_id(self, entity_id: str) -> Organization:
        """Get an organization by its ID. """
        return super().from_id(entity_id)

    async def afrom_id(self, entity_id: str) -> Organization:
        """Get an organization asynchronously by its ID. """
        return await super().afrom_id(entity_id)


class PersonManager(BaseEntityManager[Person]):
    """Manager for Person entities with improved type hinting."""

    def from_id(self, entity_id: str) -> Person:
        """Get a person by their ID. """
        return super().from_id(entity_id)

    async def afrom_id(self, entity_id: str) -> Person:
        """Get a person asynchronously by their ID. """
        return await super().afrom_id(entity_id)


class EventManager(BaseEntityManager[Event]):
    """Manager for Event entities with improved type hinting."""

    def from_id(self, entity_id: str) -> Event:
        """Get an event by its ID. """
        return super().from_id(entity_id)

    async def afrom_id(self, entity_id: str) -> Event:
        """Get an event asynchronously by its ID. """
        return await super().afrom_id(entity_id)


class RelationManager(BaseEntityManager[Relation]):
    """Manager for Relation entities with improved type hinting."""

    def from_id(self, entity_id: str) -> Relation:
        """Get a relation by its ID."""
        return super().from_id(entity_id)

    async def afrom_id(self, entity_id: str) -> Relation:
        """Get a relation asynchronously by its ID."""
        return await super().afrom_id(entity_id)
