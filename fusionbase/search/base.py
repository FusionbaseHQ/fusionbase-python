"""Base search module for Fusionbase SDK."""

from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel

from fusionbase.entities.base import Entity

T = TypeVar('T', bound=Entity)


class SearchParams(BaseModel):
    """Base class for search parameters."""

    query: Optional[str] = None
    limit: int = 10
    offset: int = 0
    filters: Dict[str, Any] = {}
    sort: Optional[Dict[str, str]] = None


class SearchResult(Generic[T]):
    """Base class for search results.

    Attributes:
        items: List of found items
        total: Total number of results found
        limit: Limit used for the search
        offset: Offset used for the search
        params: Parameters used for the search
    """

    # To silence line-too-long (C0301) on line 39:
    # pylint: disable=C0301

    def __init__(self,
                 items: List[T],
                 total: int,
                 limit: int,
                 offset: int,
                 params: Optional[SearchParams] = None):
        # Fixed too many positional arguments by making params optional with default None
        self.items = items
        self.total = total
        self.limit = limit
        self.offset = offset
        self.params = params

    def __str__(self) -> str:
        """Return string representation."""
        return f"SearchResult(total={self.total}, items={len(self.items)})"

    def __repr__(self) -> str:
        """Return representation string."""
        return self.__str__()


class BaseSearch(Generic[T]):
    """Base class for all search operations.

    Attributes:
        client: Fusionbase client
        result_class: Entity class for search results
    """

    def __init__(self, client, result_class: Type[T]):
        """Initialize a search instance.

        Args:
            client: Fusionbase client
            result_class: Entity class for search results
        """
        self.client = client
        self.result_class = result_class

    def search(self, params: Optional[SearchParams] = None) -> SearchResult[T]:
        """Perform a search operation.

        Args:
            params: Search parameters

        Returns:
            Search results

        Raises:
            APIError: If the search operation fails
        """
        # Implementation will be filled in by subclasses
        raise NotImplementedError("search not implemented in base class")

    async def asearch(self,
                      params: Optional[SearchParams] = None) -> SearchResult[T]:
        """Perform an async search operation.

        Args:
            params: Search parameters

        Returns:
            Search results

        Raises:
            APIError: If the search operation fails
        """
        # Implementation will be filled in by subclasses
        raise NotImplementedError("asearch not implemented in base class")
