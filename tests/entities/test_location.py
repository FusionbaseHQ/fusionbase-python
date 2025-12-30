"""Tests for the Location entity."""

import unittest

import pytest

from conftest import get_api_key
from fusionbase import Fusionbase
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.types.entities import LocationSubtype


class TestLocation(unittest.TestCase):
    """Test cases for Location entity."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich
        self.invalid_location_id = "non_existing_id"

        # Use API key from environment or skip tests if not available
        self.api_key = get_api_key()
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

    def test_location_from_id_real_api(self):
        """Test getting location data from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Get location using real API
            location = client.entities.locations.from_id(self.valid_location_id)

            # Assert only non-dynamic fields
            self.assertEqual(location.fb_entity_id, self.valid_location_id)
            self.assertEqual(location.formatted_address, "Munich, Germany")
            self.assertEqual(location.entity_subtype,
                             LocationSubtype.CITY_NO_POSTAL_CODE)
            self.assertEqual(location.coordinate.latitude, 48.1371079)
            self.assertEqual(location.coordinate.longitude, 11.5753822)

            # Test property accessors
            self.assertEqual(location.city, "Munich")
            self.assertEqual(location.state, "Bavaria")
            self.assertEqual(location.country, "Germany")
            self.assertIsNone(location.postal_code)

            # Verify that dynamic fields exist but don't assert their values
            self.assertIsNotNone(location.fb_entity_version)
            self.assertTrue("created_at" in location.metadata)
            self.assertTrue("updated_at" in location.metadata)
            self.assertTrue("fb_datetime" in location.metadata)

        finally:
            # Always close the client
            client.close()

    def test_not_found_error_real_api(self):
        """Test real API error handling for non-existent location."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Test that the correct exception is raised
            with self.assertRaises(ResourceNotFoundError) as context:
                client.entities.locations.from_id(self.invalid_location_id)

            # Verify exception details
            error = context.exception
            self.assertEqual(error.resource_type, "location")
            self.assertEqual(error.resource_id, self.invalid_location_id)
            self.assertEqual(error.status_code, 404)

        finally:
            # Always close the client
            client.close()


@pytest.mark.asyncio
async def test_location_async_real_api():
    """Test async location fetching with real API."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    valid_location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Fetch async
        location = await client.entities.locations.afrom_id(valid_location_id)

        # Verify key fields but not dynamic ones
        assert location.fb_entity_id == valid_location_id
        assert location.city == "Munich"
        assert location.country == "Germany"
        assert location.coordinate.latitude == 48.1371079

        # Just check dynamic fields exist
        assert location.fb_entity_version is not None
        assert "created_at" in location.metadata
        assert "updated_at" in location.metadata

    finally:
        # Close client
        client.close()
