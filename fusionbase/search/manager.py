"""Search manager for Fusionbase."""

from typing import Type

from fusionbase.search.base import BaseSearch

# For import outside toplevel:
# pylint: disable=import-outside-toplevel


class SearchManager:
    """Manager for accessing search functionality.

    This class provides access to all search types supported by Fusionbase.
    """

    def __init__(self, client):
        """Initialize the search manager with a client.

        Args:
            client: A Fusionbase client instance
        """
        self._client = client
        self._search_classes = {}

        # Initialize type-specific search managers lazily
        self._locations = None
        self._organizations = None
        self._persons = None

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
            from fusionbase.search.location_search import LocationSearch
            self._locations = LocationSearch(self._client)
        return self._locations
