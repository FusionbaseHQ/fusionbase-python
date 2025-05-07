"""Fusion Search functionality for the Fusionbase SDK."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params


class FusionSearchParams(SearchParams):
    """Parameters for fusion search.

    Attributes:
        q: Search query string
    """
    q: str  # Required


class KnowledgeGraph(BaseModel):
    """Knowledge graph information in fusion search results."""
    intent: Optional[str] = None
    from_entity_type: Optional[str] = None
    from_entity_id: Optional[str] = None
    relation_id: Optional[str] = None
    relation_parameters: Optional[Dict[str, Any]] = None


class FusionSearchResult(BaseModel):
    """Results from a fusion search.

    Attributes:
        knowledge_graph: Optional knowledge graph information if available
        results: Dictionary of results by entity type
        ranks: List of entity types in order of relevance
    """
    knowledge_graph: Optional[KnowledgeGraph] = None
    results: Dict[str, List[Dict[str, Any]]] = {}
    ranks: List[str] = []


class FusionSearch(BaseSearch):
    """Search interface for searching across all entities."""

    def __init__(self, client):
        """Initialize with a client instance."""
        super().__init__(client, None)
        self._endpoint = "search/fusion"

    def search(self, params=None, **kwargs) -> FusionSearchResult:
        """Search across all entity types.

        Args:
            params: Search parameters as FusionSearchParams object
            **kwargs: Parameters as keyword arguments

        Returns:
            FusionSearchResult containing results for different entity types

        Example:
            ```python
            # Search by query string
            result = client.search.fusion.search(q="health insurance")

            # Using parameters object
            params = FusionSearchParams(q="health insurance")
            result = client.search.fusion.search(params)

            # Access results by entity type
            organizations = result.results.get("organizations", [])
            streams = result.results.get("streams", [])
            ```
        """
        # Prepare search parameters
        search_params, query_params = prepare_search_params(params,
                                                            FusionSearchParams,
                                                            require_query=True,
                                                            **kwargs)

        # Make the search request
        response_data = make_search_request(self.client, self._endpoint,
                                            query_params)

        # Convert the response to a FusionSearchResult
        result = FusionSearchResult(
            knowledge_graph=KnowledgeGraph.model_validate(
                response_data.get("knowledge_graph", {}))
            if "knowledge_graph" in response_data else None,
            results=response_data.get("results", {}),
            ranks=response_data.get("ranks", []))

        return result

    async def asearch(self, params=None, **kwargs) -> FusionSearchResult:
        """Asynchronously search across all entity types.

        Args:
            params: Search parameters
            **kwargs: Parameters as keyword arguments

        Returns:
            FusionSearchResult containing results for different entity types

        Example:
            ```python
            # Async search
            result = await client.search.fusion.asearch(q="health insurance")

            # Access results by entity type
            organizations = result.results.get("organizations", [])
            streams = result.results.get("streams", [])
            ```
        """
        # Prepare search parameters
        search_params, query_params = prepare_search_params(params,
                                                            FusionSearchParams,
                                                            require_query=True,
                                                            **kwargs)

        # Make the async search request
        response_data = await make_search_request_async(
            self.client,
            self._endpoint,
            query_params,
            fallback_sync_method=lambda: self.search(params, **kwargs))

        # Convert the response to a FusionSearchResult
        result = FusionSearchResult(
            knowledge_graph=KnowledgeGraph.model_validate(
                response_data.get("knowledge_graph", {}))
            if "knowledge_graph" in response_data else None,
            results=response_data.get("results", {}),
            ranks=response_data.get("ranks", []))

        return result
