"""Relation search module."""

from typing import Optional

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.relation import Relation
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params
from fusionbase.utils.search_utils import process_search_results


class RelationSearchParams(SearchParams):
    """Parameters for relation search."""

    q: str  # Required
    skip: int = 0
    limit: int = 10


class RelationSearch(BaseSearch[Relation]):
    """Search for relations."""

    def __init__(self, client):
        """Initialize a relation search instance."""
        super().__init__(client, Relation)
        self._endpoint = "search/relation"

    def search(self,
               params: Optional[RelationSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Relation]]:
        """Search for relations."""
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     RelationSearchParams,
                                                     require_query=True,
                                                     **kwargs)

        # Make the request and get response data
        response_data = make_search_request(self.client, self._endpoint,
                                            query_params)

        # Process results into SearchResult - use special handling for relations
        return process_search_results(response_data,
                                      Relation,
                                      self.client,
                                      params,
                                      entity_id_field="id")

    async def asearch(self,
                      params: Optional[RelationSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Relation]]:
        """Search for relations asynchronously."""
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     RelationSearchParams,
                                                     require_query=True,
                                                     **kwargs)

        # Create a closure for the fallback sync method
        def fallback_sync():
            return self.search(params)

        # Make the async request
        response_data = await make_search_request_async(self.client,
                                                        self._endpoint,
                                                        query_params,
                                                        fallback_sync)

        # Process results into SearchResult - use special handling for relations
        return process_search_results(response_data,
                                      Relation,
                                      self.client,
                                      params,
                                      entity_id_field="id")
