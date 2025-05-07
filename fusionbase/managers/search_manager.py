"""Search manager for Fusionbase."""

from typing import Type, TypeVar

from fusionbase.managers.search_wrappers import DataSearch
from fusionbase.managers.search_wrappers import FusionSearch
from fusionbase.managers.search_wrappers import LocationSearch
from fusionbase.managers.search_wrappers import OrganizationSearch
from fusionbase.managers.search_wrappers import PersonSearch
from fusionbase.managers.search_wrappers import RelationSearch
from fusionbase.search.base import BaseSearch

# Type parameter for better IDE support
T = TypeVar('T')


class SearchManager:
    """Manager for accessing search functionality.

    This class provides access to all search types supported by Fusionbase.
    """

    def __init__(self, client):
        """Initialize the search manager."""
        self._client = client
        self._search_classes = {}
        self._locations = None
        self._persons = None
        self._organizations = None
        self._relations = None
        self._data = None
        self._fusion = None

        # For async versions (separate instances to avoid state conflicts)
        self._async_loc_search = None
        self._async_person_search = None
        self._async_org_search = None
        self._async_rel_search = None
        self._async_data_search = None
        self._async_fusion_search = None

    def register_search_class(self, search_type: str,
                              cls: Type[BaseSearch]) -> None:
        """Register a search class with the manager.

        Args:
            search_type: The type identifier for the search
            cls: The search class
        """
        self._search_classes[search_type] = cls

    @property
    def locations(self):
        """Get the locations search.

        Returns:
            A search manager for location entities
        """
        if self._locations is None:
            self._locations = LocationSearch(self._client)
        return self._locations

    @property
    def persons(self):
        """Get the persons search.

        Returns:
            A search manager for person entities
        """
        if self._persons is None:
            self._persons = PersonSearch(self._client)
        return self._persons

    @property
    def organizations(self):
        """Get the organizations search.

        Returns:
            A search manager for organization entities
        """
        if self._organizations is None:
            self._organizations = OrganizationSearch(self._client)
        return self._organizations

    @property
    def relations(self):
        """Get the relations search.

        Returns:
            A search manager for relation entities
        """
        if self._relations is None:
            self._relations = RelationSearch(self._client)
        return self._relations

    @property
    def data(self):
        """Get the data search.

        Returns:
            A search manager for data entities (streams and services)
        """
        if self._data is None:
            self._data = DataSearch(self._client)
        return self._data

    @property
    def fusion(self):
        """Get the fusion search.

        Returns:
            A search manager for fusion search across all entities
        """
        if self._fusion is None:
            self._fusion = FusionSearch(self._client)
        return self._fusion

    # Add async methods directly on the SearchManager
    async def asearch_persons(self, params=None, **kwargs):
        """Search for persons asynchronously."""
        if self._async_person_search is None:
            self._async_person_search = PersonSearch(self._client)
        return await self._async_person_search.asearch(params, **kwargs)

    async def asearch_organizations(self, params=None, **kwargs):
        """Search for organizations asynchronously."""
        if self._async_org_search is None:
            self._async_org_search = OrganizationSearch(self._client)
        return await self._async_org_search.asearch(params, **kwargs)

    async def asearch_locations(self, params=None, **kwargs):
        """Search for locations asynchronously."""
        if self._async_loc_search is None:
            self._async_loc_search = LocationSearch(self._client)
        return await self._async_loc_search.asearch(params, **kwargs)

    async def asearch_relations(self, params=None, **kwargs):
        """Search for relations asynchronously."""
        if self._async_rel_search is None:
            self._async_rel_search = RelationSearch(self._client)
        return await self._async_rel_search.asearch(params, **kwargs)

    async def asearch_data(self, params=None, **kwargs):
        """Search for data (streams and services) asynchronously."""
        if self._async_data_search is None:
            self._async_data_search = DataSearch(self._client)
        return await self._async_data_search.asearch(params, **kwargs)

    async def asearch_fusion(self, params=None, **kwargs):
        """Search across all entities asynchronously."""
        if self._async_fusion_search is None:
            self._async_fusion_search = FusionSearch(self._client)
        return await self._async_fusion_search.asearch(params, **kwargs)
