"""Specialized search managers for better type hinting."""

from fusionbase.search.location_search import LocationSearch as BaseLocationSearch
from fusionbase.search.organization_search import \
    OrganizationSearch as BaseOrganizationSearch
from fusionbase.search.person_search import PersonSearch as BasePersonSearch


class LocationSearch(BaseLocationSearch):
    """Search manager for Location entities with improved type hinting."""


class OrganizationSearch(BaseOrganizationSearch):
    """Search manager for Organization entities with improved type hinting."""


class PersonSearch(BasePersonSearch):
    """Search manager for Person entities with improved type hinting."""
