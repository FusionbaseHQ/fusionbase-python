"""Location search module."""

from typing import Optional

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.location import Location
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params
from fusionbase.utils.search_utils import process_search_results


class LocationSearchParams(SearchParams):
    """Parameters for location search."""

    q: str  # Required
    skip: int = 0
    limit: int = 10


class LocationSearch(BaseSearch[Location]):
    """Search for locations."""

    def __init__(self, client):
        """Initialize a location search instance."""
        super().__init__(client, Location)
        self._endpoint = "search/entities/location"

    def search(self,
               q: str,
               params: Optional[LocationSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Location]]:
        """Search for locations.

        Args:
            q: Search query string (required)
            params: Search parameters as LocationSearchParams object
            **kwargs: Additional parameters as keyword arguments
        """
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     LocationSearchParams,
                                                     require_query=True,
                                                     q=q,
                                                     **kwargs)

        # Make the request and get response data
        response_data = make_search_request(self.client, self._endpoint,
                                            query_params)

        # Process results into SearchResult
        return process_search_results(response_data, Location, self.client,
                                      params)

    async def asearch(self,
                      q: str,
                      params: Optional[LocationSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Location]]:
        """Search for locations asynchronously.

        Args:
            q: Search query string (required)
            params: Search parameters as LocationSearchParams object
            **kwargs: Additional parameters as keyword arguments
        """
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     LocationSearchParams,
                                                     require_query=True,
                                                     q=q,
                                                     **kwargs)

        # Create a closure for the fallback sync method
        def fallback_sync():
            return self.search(q, params)

        # Make the async request
        response_data = await make_search_request_async(self.client,
                                                        self._endpoint,
                                                        query_params,
                                                        fallback_sync)

        # Process results into SearchResult
        return process_search_results(response_data, Location, self.client,
                                      params)
