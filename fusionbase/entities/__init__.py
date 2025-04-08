"""Entity classes for Fusionbase API."""

from fusionbase.entities.base import Entity
from fusionbase.entities.event import Event
from fusionbase.entities.feature import Feature
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person
from fusionbase.entities.relation import Relation

__all__ = [
    "Entity", "Location", "Person", "Organization", "Event", "Feature",
    "Relation"
]
