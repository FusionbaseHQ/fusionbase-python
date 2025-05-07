"""Organization search module."""

from typing import Any, Dict, Optional

from pydantic import field_validator

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.organization import Organization
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.types.entities import FilterKey
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params
from fusionbase.utils.search_utils import process_search_results


class OrganizationSearchParams(SearchParams):
    """Parameters for organization search."""

    q: str  # Required
    source_key: Optional[str] = None
    skip: int = 0
    limit: int = 10
    filters: Optional[Dict[FilterKey, Any]] = None

    @field_validator('filters')
    @classmethod
    def validate_filter_keys(cls, v):
        """Ensure filter keys are all FilterKey enums."""
        if v is None:
            return v

        # Create a new dict with validated keys
        validated_filters = {}
        for key, value in v.items():
            # If key is already a FilterKey enum, use it directly
            if isinstance(key, FilterKey):
                validated_filters[key] = value
            else:
                # Attempt to convert string keys to FilterKey
                try:
                    enum_key = FilterKey(key)
                    validated_filters[enum_key] = value
                except ValueError:
                    raise ValueError(
                        f"Invalid filter key: '{key}'. Must be a FilterKey enum value. "
                        f"Valid values: {', '.join([f.value for f in FilterKey])}"
                    )

        return validated_filters


class OrganizationSearch(BaseSearch[Organization]):
    """Search for organizations."""

    def __init__(self, client):
        """Initialize a new organization search instance."""
        super().__init__(client, Organization)
        self._endpoint = "search/entities/organization"

    def search(self,
               params: Optional[OrganizationSearchParams] = None,
               **kwargs) -> SearchResult[LazyReference[Organization]]:
        """Search for organizations with given parameters."""
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     OrganizationSearchParams,
                                                     require_query=True,
                                                     **kwargs)

        # Make the request and get response data
        response_data = make_search_request(self.client, self._endpoint,
                                            query_params)

        # Process results into SearchResult
        return process_search_results(response_data, Organization, self.client,
                                      params)

    async def asearch(self,
                      params: Optional[OrganizationSearchParams] = None,
                      **kwargs) -> SearchResult[LazyReference[Organization]]:
        """Search for organizations asynchronously."""
        # Prepare parameters
        params, query_params = prepare_search_params(params,
                                                     OrganizationSearchParams,
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

        # Process results into SearchResult
        return process_search_results(response_data, Organization, self.client,
                                      params)
