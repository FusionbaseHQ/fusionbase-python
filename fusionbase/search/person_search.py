"""Person search module."""

from typing import Optional

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.person import Person
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params
from fusionbase.utils.search_utils import process_search_results


class PersonSearchParams(SearchParams):
    """Parameters for person search.

    Attributes:
        q: Query string for searching persons (required)
        source_key: Data source identifier for filtering results
        skip: Number of results to skip (for pagination)
        limit: Maximum number of results to return
    """

    q: str  # Required
    source_key: Optional[str] = None
    skip: int = 0
    limit: int = 10


class PersonSearch(BaseSearch[Person]):
    """Search for persons.

    This class provides methods to search for persons in the Fusionbase platform.
    Search results contain LazyReference objects to Person entities that are only
    loaded when accessed.
    """

    def __init__(self, client):
        """Initialize a new person search instance."""
        super().__init__(client, Person)
        self._endpoint = "search/entities/person"

    def search(self,
               q: str,
               params: Optional[PersonSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Person]]:
        """Search for persons with given parameters.

        Args:
            q: Search query string (required)
            params: Search parameters as PersonSearchParams object
            **kwargs: Additional parameters as keyword arguments
        """
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     PersonSearchParams,
                                                     require_query=True,
                                                     q=q,
                                                     **kwargs)

        # Make the request and get response data
        response_data = make_search_request(self.client, self._endpoint,
                                            query_params)

        # Process results into SearchResult
        return process_search_results(response_data, Person, self.client,
                                      params)

    async def asearch(self,
                      q: str,
                      params: Optional[PersonSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Person]]:
        """Search for persons asynchronously with given parameters.

        Args:
            q: Search query string (required)
            params: Search parameters as PersonSearchParams object
            **kwargs: Additional parameters as keyword arguments
        """
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     PersonSearchParams,
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
        return process_search_results(response_data, Person, self.client,
                                      params)
