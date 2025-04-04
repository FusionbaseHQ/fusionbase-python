"""Entity classes for Fusionbase API."""

from fusionbase.entities.base import Entity
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person

__all__ = ["Entity", "Location", "Person", "Organization"]
