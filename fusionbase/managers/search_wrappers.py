"""Specialized search managers for better type hinting."""

from fusionbase.search.data_search import DataSearch as BaseDataSearch
from fusionbase.search.fusion_search import FusionSearch as BaseFusionSearch
from fusionbase.search.location_search import LocationSearch as BaseLocationSearch
from fusionbase.search.organization_search import OrganizationSearch as BaseOrganizationSearch
from fusionbase.search.person_search import PersonSearch as BasePersonSearch
from fusionbase.search.relation_search import RelationSearch as BaseRelationSearch


class LocationSearch(BaseLocationSearch):
    """Search manager for Location entities with improved type hinting."""


class OrganizationSearch(BaseOrganizationSearch):
    """Search manager for Organization entities with improved type hinting."""


class PersonSearch(BasePersonSearch):
    """Search manager for Person entities with improved type hinting."""


class RelationSearch(BaseRelationSearch):
    """Search manager for Relation entities with improved type hinting."""


class DataSearch(BaseDataSearch):
    """Search manager for Data entities (streams and services) with improved type hinting."""


class FusionSearch(BaseFusionSearch):
    """Search manager for fusion search across all entities with improved type hinting."""
