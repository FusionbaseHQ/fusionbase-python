"""Fusionbase Python SDK for fast access to the world's data."""

from fusionbase.client import Fusionbase
from fusionbase.config import FusionbaseConfig
from fusionbase.entities import Entity
from fusionbase.entities import Location
from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import FusionbaseError
from fusionbase.logging import configure_logging
from fusionbase.search import BaseSearch
from fusionbase.search import LocationSearch
from fusionbase.search import LocationSearchParams
from fusionbase.search import SearchParams
from fusionbase.search import SearchResult

__version__ = "0.3.0"

__all__ = [
    # Client and core functionality
    "Fusionbase",
    "FusionbaseConfig",
    "configure_logging",

    # Exceptions
    "FusionbaseError",
    "AuthenticationError",
    "APIError",

    # Entities
    "Entity",
    "Location",

    # Search
    "SearchParams",
    "SearchResult",
    "BaseSearch",
    "LocationSearch",
    "LocationSearchParams",
]
