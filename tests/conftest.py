"""Shared fixtures and configuration for tests."""

import os
from unittest.mock import MagicMock

import pytest

from fusionbase import Fusionbase
from fusionbase.config import FusionbaseConfig


@pytest.fixture
def mock_response():
    """Create a mock HTTP response."""
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.status_code = 200
    return mock


@pytest.fixture
def api_key():
    """Get the API key from environment or use a test one."""
    return os.environ.get("FUSIONBASE_API_KEY", "test_api_key")


@pytest.fixture
def client(api_key):
    """Create a Fusionbase client with the real or test API key."""
    client = Fusionbase(api_key=api_key)
    yield client
    client.close()


@pytest.fixture
def test_config():
    """Create a test configuration."""
    return FusionbaseConfig(
        timeout=5.0,
        cache={"enabled": False},
        retry={"enabled": False},
    )


@pytest.fixture
def test_location_data():
    """Return test location data for mocking."""
    return {
        "fb_entity_id": "bfcc19ddd9edb12efb9cfea181b0dcd3",
        "fb_entity_version": "test_version",
        "entity_type": "LOCATION",
        "entity_subtype": "CITY_NO_POSTAL_CODE",
        "coordinate": {
            "latitude": 48.1371079,
            "longitude": 11.5753822
        },
        "address_components": [{
            "component_type": "city",
            "component_value": "Munich"
        }, {
            "component_type": "state",
            "component_value": "Bavaria"
        }, {
            "component_type": "country",
            "component_value": "Germany"
        }],
        "formatted_address": "Munich, Germany",
        "updated_at": "2023-01-01T00:00:00.000000",
        "created_at": "2023-01-01T00:00:00.000000",
        "fb_datetime": "2023-01-01T00:00:00.000000"
    }
