"""Tests for Location search functionality."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.location import Location
from fusionbase.search.location_search import LocationSearchParams


class TestLocationSearch(unittest.TestCase):
    """Test cases for Location search functionality."""

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

    def test_location_search_by_query(self):
        """Test searching for locations by query."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search for Munich
        params = LocationSearchParams(q="Munich, Germany")
        results = self.client.search.locations.search(params)

        # Verify we have results
        self.assertGreater(len(results.items), 0)

        # Check the first result is a Location object
        first_result = results.items[0]
        self.assertIsInstance(first_result, Location)

        # Verify it has basic location properties
        self.assertIsNotNone(first_result.fb_entity_id)
        self.assertIsNotNone(first_result.formatted_address)
        self.assertIsNotNone(first_result.coordinate)

        # Check that the result contains "Munich" in the city or address
        found_munich = False
        if first_result.city and "München" in first_result.city:
            found_munich = True
        elif first_result.formatted_address and "Munich" in first_result.formatted_address:
            found_munich = True
        self.assertTrue(found_munich, "Munich not found in search results")

    def test_location_search_with_limit(self):
        """Test searching for locations with a limit."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search with limit=1
        params = LocationSearchParams(q="Berlin", limit=1)
        results = self.client.search.locations.search(params)

        # Verify we have exactly 1 result
        self.assertEqual(len(results.items), 1)

        # Verify it's a valid location
        location = results.items[0]
        self.assertIsInstance(location, Location)
        self.assertIsNotNone(location.fb_entity_id)


@pytest.mark.asyncio
async def test_location_search_async():
    """Test async location searching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)
    # Get the async client explicitly
    async_client = client.async_client

    try:
        # Search for a specific address asynchronously
        params = LocationSearchParams(q="Agnes-Pockels-Bogen 1, 80992 München")

        try:

            # Use async client's search manager
            results = await async_client.search.locations.asearch(params)
            print(f"Received results: {results}")

            # Verify we have results
            assert len(results.items) > 0

            # Check first result
            location = results.items[0]
            assert location.fb_entity_id is not None
            assert location.formatted_address is not None
            assert location.coordinate is not None
            assert location.coordinate.latitude is not None
            assert location.coordinate.longitude is not None
        except Exception as e:
            import traceback
            print(f"Error during async test: {type(e).__name__}: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise

    finally:
        # Close client
        await async_client.aclose()
