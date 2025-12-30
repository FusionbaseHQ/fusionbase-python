"""Fusionbase Python SDK."""

import importlib.metadata

from fusionbase.core.client import Fusionbase as CoreFusionbase
from fusionbase.core.config import FusionbaseConfig
from fusionbase.data.datastream import DataStream
from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import AuthorizationError
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import RequestValidationError as ValidationError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.managers.dataservice_manager import DataServiceManager
from fusionbase.managers.datastream_manager import DataStreamManager
from fusionbase.search import LocationSearch
from fusionbase.search import LocationSearchParams
from fusionbase.search import OrganizationSearch
from fusionbase.search import OrganizationSearchParams
from fusionbase.search import PersonSearch
from fusionbase.search import PersonSearchParams
from fusionbase.search import RelationSearch
from fusionbase.search import RelationSearchParams

try:
    __version__ = importlib.metadata.version("fusionbase")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.8.0"  # Fallback for development


class Fusionbase(CoreFusionbase):
    """Main client for the Fusionbase Python SDK.

    This class inherits from the core Fusionbase client implementation
    and adds additional convenience methods.
    """

    def __init__(self, api_key=None, **kwargs):
        """Initialize the Fusionbase client.

        Args:
            api_key: API key for authentication
            **kwargs: Additional arguments passed to the core client
        """
        super().__init__(api_key=api_key, **kwargs)
        # Initialize the managers
        self.services = DataServiceManager(self)
        self.streams = DataStreamManager(self)

    def __enter__(self):
        """Context manager entry."""
        # Call the parent's __enter__ if it exists, otherwise return self
        if hasattr(super(), "__enter__"):
            return super().__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Call the parent's __exit__ if it exists
        if hasattr(super(), "__exit__"):
            return super().__exit__(exc_type, exc_val, exc_tb)
        self.close()

    async def __aenter__(self):
        """Async context manager entry."""
        # Call the parent's __aenter__ if it exists, otherwise return self
        if hasattr(super(), "__aenter__"):
            return await super().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        # Call the parent's __aexit__ if it exists
        if hasattr(super(), "__aexit__"):
            return await super().__aexit__(exc_type, exc_val, exc_tb)
        await self.aclose()


__all__ = [
    '__version__',
    'Fusionbase', 'FusionbaseConfig', 'APIError', 'AuthenticationError',
    'AuthorizationError', 'FusionbaseError', 'ResourceNotFoundError',
    'ValidationError', 'LocationSearch', 'LocationSearchParams',
    'OrganizationSearch', 'OrganizationSearchParams', 'PersonSearch',
    'PersonSearchParams', 'RelationSearch', 'RelationSearchParams', 'DataStream'
]
