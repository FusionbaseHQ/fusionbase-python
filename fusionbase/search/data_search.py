"""Search functionality for DataStream and DataService entities."""

from typing import Any, Dict, Optional, TypeVar, Union

from fusionbase.data.dataservice import DataService
from fusionbase.data.datastream import DataStream
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params

T = TypeVar('T', bound=Union[DataStream, DataService])


class DataSearchParams(SearchParams):
    """Parameters for data search.

    Attributes:
        q: Search query string
    """
    q: Optional[str] = None


class DataSearch(BaseSearch):
    """Search interface for data entities (streams and services)."""

    def __init__(self, client):
        """Initialize with a client instance."""
        # Pass None as entity_class since we'll determine the type during processing
        super().__init__(client, None)
        self._endpoint = "search/data"

    def _process_search_results(
        self, response_data: Dict[str, Any], search_params: SearchParams
    ) -> SearchResult[LazyReference[Union[DataStream, DataService]]]:
        """Process search results to handle both stream and service types."""
        if "results" not in response_data:
            return SearchResult(
                items=[],
                total=0,
                limit=0,
                skip=0,
                params=search_params,
            )

        # Process results
        entities = []
        for result in response_data.get("results", []):
            if not isinstance(result, dict) or "entity" not in result:
                continue

            entity_data = result.get("entity", {})
            entity_id = entity_data.get("key")
            if not entity_id:
                continue

            # Determine entity type based on "type" field
            entity_type = entity_data.get("type", "stream")
            if entity_type == "service":
                entity_class = DataService
            else:
                entity_class = DataStream

            # Create the appropriate LazyReference
            entity_ref = LazyReference(entity_id, entity_class, self.client)
            entities.append(entity_ref)

        return SearchResult(
            items=entities,
            total=response_data.get("total", len(entities)),
            limit=len(entities),  # Use actual number of items as the limit
            skip=0,
            params=search_params,
        )

    def search(self,
               q: str,
               params: Optional[DataSearchParams] = None,
               **kwargs
              ) -> SearchResult[LazyReference[Union[DataStream, DataService]]]:
        """Search for data streams and services.

        Args:
            q: Search query string (required)
            params: Search parameters as DataSearchParams object
            **kwargs: Additional parameters as keyword arguments

        Returns:
            SearchResult containing LazyReferences to DataStream or DataService objects

        Example:
            ```python
            # Search by query string
            result = client.search.data.search("financial")

            # With additional parameters
            result = client.search.data.search("financial", limit=20)

            # Iterate through results
            for item_ref in result.items:
                item = item_ref.get()  # Fetch the full object
                if isinstance(item, DataStream):
                    print(f"Found stream: {item.get_metadata().display_name}")
                elif isinstance(item, DataService):
                    print(f"Found service: {item.name}")
            ```
        """
        # Prepare search parameters - require a query to avoid empty searches
        search_params, query_params = prepare_search_params(params,
                                                            DataSearchParams,
                                                            require_query=True,
                                                            q=q,
                                                            **kwargs)

        # Make the search request - Use self.client (NOT self._client)
        response_data = make_search_request(self.client, self._endpoint,
                                            query_params)

        # Use our custom processing method to handle both streams and services
        return self._process_search_results(response_data, search_params)

    async def asearch(
        self,
        q: str,
        params: Optional[DataSearchParams] = None,
        **kwargs
    ) -> SearchResult[LazyReference[Union[DataStream, DataService]]]:
        """Asynchronously search for data streams and services.

        Args:
            q: Search query string (required)
            params: Search parameters
            **kwargs: Additional parameters as keyword arguments

        Returns:
            SearchResult containing LazyReferences to DataStream or DataService objects

        Example:
            ```python
            # Async search
            result = await client.search.data.asearch("financial")

            # Process results asynchronously
            for item_ref in result.items:
                item = await item_ref.aget()
                if isinstance(item, DataStream):
                    print(f"Found stream: {item.get_metadata().display_name}")
                elif isinstance(item, DataService):
                    print(f"Found service: {item.name}")
            ```
        """
        # Prepare search parameters - require a query to avoid empty searches
        search_params, query_params = prepare_search_params(params,
                                                            DataSearchParams,
                                                            require_query=True,
                                                            q=q,
                                                            **kwargs)

        # Make the async search request - Use self.client (NOT self._client)
        response_data = await make_search_request_async(
            self.client,
            self._endpoint,
            query_params,
            fallback_sync_method=lambda: self.search(q, params, **kwargs))

        # Use our custom processing method to handle both streams and services
        return self._process_search_results(response_data, search_params)
