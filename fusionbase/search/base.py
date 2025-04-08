"""Base classes for search functionality."""

from abc import ABC
from typing import Any, Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict

# Make the type parameter have a more restrictive bound for better typing
from fusionbase.entities.base import Entity

T = TypeVar('T', bound=Entity)


class SearchParams(BaseModel):
    """Base class for search parameters."""

    model_config = ConfigDict(extra="ignore")


class SearchResult(Generic[T]):
    """Search result container.

    Attributes:
        items: List of result items
        total: Total number of results available on the server
        limit: Maximum number of results per page
        skip: Number of results to skip (for pagination)
        params: Search parameters used for this search
    """

    def __init__(self,
                 items: List[Any],
                 total: int,
                 limit: int,
                 skip: int,
                 params: Optional[SearchParams] = None):
        """Initialize a search result.

        Args:
            items: List of result items
            total: Total number of results available
            limit: Maximum number of results per page
            skip: Number of results to skip (for pagination)
            params: Search parameters used for this search
        """
        self.items = items
        self.total = total
        self.limit = limit
        self.skip = skip
        self.params = params

    def __len__(self) -> int:
        """Get the number of items in the result."""
        return len(self.items)

    def __getitem__(self, idx) -> Any:
        """Get an item by index."""
        return self.items[idx]

    def __iter__(self):
        """Iterate through items."""
        return iter(self.items)


class BaseSearch(Generic[T], ABC):
    """Base class for search operations.

    This is used as a base class for all entity search implementations.
    Search results now return LazyReference objects that are only loaded when accessed.
    """

    def __init__(self, client: Any, entity_class: Type[T]):
        """Initialize a base search instance.

        Args:
            client: The client to use for API requests
            entity_class: The class of entity being searched
        """
        self.client = client
        self.entity_class = entity_class
