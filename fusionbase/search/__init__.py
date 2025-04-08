"""Search functionality for the Fusionbase SDK."""

# Import search classes directly from their implementation files
# instead of from the wrappers to avoid circular imports
from fusionbase.search.location_search import LocationSearch
from fusionbase.search.location_search import LocationSearchParams
from fusionbase.search.organization_search import OrganizationSearch
from fusionbase.search.organization_search import OrganizationSearchParams
from fusionbase.search.person_search import PersonSearch
from fusionbase.search.person_search import PersonSearchParams

__all__ = [
    'LocationSearch',
    'OrganizationSearch',
    'PersonSearch',
    'LocationSearchParams',
    'OrganizationSearchParams',
    'PersonSearchParams',
]
