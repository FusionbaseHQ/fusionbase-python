"""Search functionality for Fusionbase."""

from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.search.location_search import LocationSearch
from fusionbase.search.location_search import LocationSearchParams
from fusionbase.search.manager import SearchManager
from fusionbase.search.person_search import PersonSearch
from fusionbase.search.person_search import PersonSearchParams

__all__ = [
    'BaseSearch', 'SearchParams', 'SearchResult', 'LocationSearch',
    'LocationSearchParams', 'PersonSearch', 'PersonSearchParams',
    'SearchManager'
]
