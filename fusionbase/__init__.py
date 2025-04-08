"""Fusionbase Python client.

This package provides a Python interface to the Fusionbase Data Hub.
"""

from fusionbase.core.client import Fusionbase
from fusionbase.core.config import FusionbaseConfig
from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import AuthorizationError
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import RequestValidationError as ValidationError
from fusionbase.exceptions import ResourceNotFoundError
# Ensure all search types are available for import from the package
from fusionbase.search import LocationSearch
from fusionbase.search import LocationSearchParams
from fusionbase.search import OrganizationSearch
from fusionbase.search import OrganizationSearchParams
from fusionbase.search import PersonSearch
from fusionbase.search import PersonSearchParams
from fusionbase.search import RelationSearch
from fusionbase.search import RelationSearchParams

__all__ = [
    'Fusionbase', 'FusionbaseConfig', 'APIError', 'AuthenticationError',
    'AuthorizationError', 'FusionbaseError', 'ResourceNotFoundError',
    'ValidationError', 'LocationSearch', 'LocationSearchParams',
    'OrganizationSearch', 'OrganizationSearchParams', 'PersonSearch',
    'PersonSearchParams', 'RelationSearch', 'RelationSearchParams'
]
