"""Tests for Data search functionality (streams and services)."""

import os
import unittest
from unittest.mock import patch

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.search.data_search import DataSearchParams


class TestDataSearch(unittest.TestCase):
    """Test cases for Data search functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_data_search_mock(self):
        """Test searching for data with mocked response."""
        # Create a mock response
        mock_response = {
            "results": [{
                "entity": {
                    "key": "12345",
                    "name": {
                        "en": "Financial Data Stream"
                    },
                    "description": {
                        "en": "Stream with financial data"
                    },
                    "type": "stream"
                },
                "score": 0.95
            }, {
                "entity": {
                    "key": "67890",
                    "name": {
                        "en": "Geocoding Service"
                    },
                    "description": {
                        "en": "Service for geocoding addresses"
                    },
                    "type": "service"
                },
                "score": 0.90
            }],
            "total": 2
        }

        # Create a mock for the client.request method instead
        with patch.object(self.client, 'request',
                          return_value=mock_response) as mock_request:
            # Search for data
            params = DataSearchParams(q="financial")
            results = self.client.search.data.search(params)

            # Verify the mock was called
            mock_request.assert_called_once()

            # Verify search results
            self.assertEqual(len(results.items), 2)
            self.assertEqual(results.total, 2)

            # Check that items are LazyReferences
            self.assertIsInstance(results.items[0], LazyReference)
            self.assertEqual(results.items[0].entity_id, "12345")
            self.assertEqual(results.items[1].entity_id, "67890")

            # Check that the request was made with the correct parameters
            args, kwargs = mock_request.call_args
            self.assertEqual(args[0], "GET")
            self.assertEqual(args[1], "search/data")
            self.assertEqual(kwargs.get("params", {}).get("q"), "financial")

    def test_data_search_real_api(self):
        """Test searching for data with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search for a generic term that should match some data
        params = DataSearchParams(q="test")
        results = self.client.search.data.search(params)

        # Verify we have results or at least no errors
        self.assertIsInstance(results.items, list)

        if len(results.items) > 0:
            # Check the first result is a LazyReference
            first_result = results.items[0]
            self.assertIsInstance(first_result, LazyReference)

            # Verify it has an entity ID
            self.assertIsNotNone(first_result.entity_id)


@pytest.mark.asyncio
async def test_data_search_async():
    """Test async data searching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Search for data asynchronously
        params = DataSearchParams(q="test")

        try:
            # Use async methods directly on the client
            results = await client.search.data.asearch(params)

            # Verify we have results (or at least the call succeeded)
            assert isinstance(results.items, list)

            if len(results.items) > 0:
                # Check first result
                data_ref = results.items[0]
                assert data_ref.entity_id is not None

        except Exception as e:
            pytest.fail(f"Async search failed with error: {e}")

    finally:
        # Close client (close both sync and async resources)
        await client.aclose()
        client.close()
