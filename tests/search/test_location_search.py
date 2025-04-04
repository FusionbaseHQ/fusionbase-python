"""Tests for Location search functionality."""

import os
import traceback
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
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

        # Search for a location
        params = LocationSearchParams(q="München")
        results = self.client.search.locations.search(params)

        # Verify we have results
        self.assertGreater(len(results.items), 0)

        # Check the first result is a LazyReference to Location (not directly a Location)
        first_result = results.items[0]
        self.assertIsInstance(first_result, LazyReference)

        # Verify it has the entity_id property from LazyReference
        self.assertIsNotNone(first_result.entity_id)

        # Load the entity and check specific properties
        location = first_result.get()
        self.assertIsInstance(location, Location)
        self.assertIsNotNone(location.formatted_address)


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
        # Search for locations asynchronously
        params = LocationSearchParams(q="Berlin")

        try:
            # Use async client's search manager
            results = await async_client.search.locations.asearch(params)

            # Verify we have results
            assert len(results.items) > 0

            # Check first result is a LazyReference
            location_ref = results.items[0]
            assert isinstance(location_ref, LazyReference)
            assert location_ref.entity_id is not None

            try:
                # Explicitly load the location with better error reporting
                location = await location_ref.aget()
                assert isinstance(location, Location)
                assert location.formatted_address is not None
            except Exception as load_error:
                print(f"Error loading location: {load_error}")
                traceback.print_exc()
                pytest.fail(f"Failed to load location: {load_error}")

        except Exception as e:
            print(f"Async search error: {e}")
            traceback.print_exc()
            pytest.fail(f"Async search failed with error: {e}")

    finally:
        # Close client
        await async_client.aclose()
        client.close()
