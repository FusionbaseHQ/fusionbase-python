"""Tests for the Person entity."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.types import PersonSubtype
from fusionbase.exceptions import ResourceNotFoundError


class TestPerson(unittest.TestCase):
    """Test cases for Person entity."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_person_id = "e6ce61d930d72a0659c066fa37ca42c7"  # Sample person ID
        self.invalid_person_id = "non_existing_id"

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

    def test_person_from_id_real_api(self):
        """Test getting person data from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Get person using real API
            person = client.entities.persons.from_id(self.valid_person_id)

            # Assert only non-dynamic fields
            self.assertEqual(person.fb_entity_id, self.valid_person_id)
            self.assertEqual(person.entity_subtype, PersonSubtype.INDIVIDUAL)

            # Test name components
            self.assertEqual(person.name.given, "Patrick")
            self.assertEqual(person.name.family, "Holl")
            self.assertIsNone(person.name.maiden)

            # Test property accessors
            self.assertEqual(person.given_name, "Patrick")
            self.assertEqual(person.family_name, "Holl")

            # Test home location if exists
            if person.home_location:
                self.assertIsNotNone(person.home_location.formatted_address)
                self.assertIsNotNone(person.home_location.coordinate)

            # Verify that dynamic fields exist but don't assert their values
            self.assertIsNotNone(person.fb_entity_version)

        finally:
            # Always close the client
            client.close()

    def test_not_found_error_real_api(self):
        """Test real API error handling for non-existent person."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Test that the correct exception is raised
            with self.assertRaises(ResourceNotFoundError) as context:
                client.entities.persons.from_id(self.invalid_person_id)

            # Verify exception details
            error = context.exception
            self.assertEqual(error.resource_type, "person")
            self.assertEqual(error.resource_id, self.invalid_person_id)
            self.assertEqual(error.status_code, 404)

        finally:
            # Always close the client
            client.close()


@pytest.mark.asyncio
async def test_person_async_real_api():
    """Test async person fetching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    valid_person_id = "e6ce61d930d72a0659c066fa37ca42c7"  # Sample person ID

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Fetch async
        person = await client.entities.persons.afrom_id(valid_person_id)

        # Verify key fields but not dynamic ones
        assert person.fb_entity_id == valid_person_id
        assert person.given_name == "Patrick"
        assert person.family_name == "Holl"

    finally:
        # Close client
        client.close()
