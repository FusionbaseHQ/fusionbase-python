"""Tests for the Event entity."""

import unittest

import pytest
from conftest import get_api_key

from fusionbase import Fusionbase
from fusionbase.exceptions import ResourceNotFoundError


class TestEvent(unittest.TestCase):
    """Test cases for Event entity."""

    def setUp(self):
        """Set up test fixtures."""
        # Use a real event ID that exists in your system
        self.valid_event_id = "9befc075e19843dba4ed7dcfc3b70dc5"  # Replace with valid ID
        self.invalid_event_id = "non_existing_id"

        # Use API key from environment or skip tests if not available
        self.api_key = get_api_key()
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

    def test_event_from_id_real_api(self):
        """Test getting event data from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Get event using real API
            event = client.entities.events.from_id(self.valid_event_id)

            # Assert only non-dynamic fields
            self.assertEqual(event.fb_entity_id, self.valid_event_id)
            self.assertIsNotNone(event.entity_subtype)

            # Test helper properties
            if event.name and (event.name.en or event.name.de):
                self.assertIsNotNone(event.event_title)

            if event.description and event.description.short:
                if event.description.short.en or event.description.short.de:
                    self.assertIsNotNone(event.event_description)

            # Test linked entities if available
            if event.linked_person:
                self.assertIsNotNone(event.linked_person.fb_entity_id)

            if event.linked_organization:
                self.assertIsNotNone(event.linked_organization.fb_entity_id)

            # Test locations if available
            for location_field in [
                    'origin_location', 'event_location', 'effect_location'
            ]:
                location = getattr(event, location_field, None)
                if location:
                    self.assertIsNotNone(location.formatted_address)

        finally:
            # Always close the client
            client.close()

    def test_not_found_error_real_api(self):
        """Test real API error handling for non-existent event."""

        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Test that the correct exception is raised
            with self.assertRaises(ResourceNotFoundError) as context:
                client.entities.events.from_id(self.invalid_event_id)

            # Verify exception details
            error = context.exception
            self.assertEqual(error.resource_type, "event")
            self.assertEqual(error.resource_id, self.invalid_event_id)
            self.assertEqual(error.status_code, 404)

        finally:
            # Always close the client
            client.close()


@pytest.mark.asyncio
async def test_event_async_real_api():
    """Test async event fetching with real API."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Use a real event ID that exists in your system
    valid_event_id = "9befc075e19843dba4ed7dcfc3b70dc5"

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Fetch async
        event = await client.entities.events.afrom_id(valid_event_id)

        # Verify key fields
        assert event.fb_entity_id == valid_event_id
        assert event.entity_subtype is not None

        # If linked entities exist, test async getters
        if event.details and event.details.linked_entities:
            if event.details.linked_entities.person:
                person = await event.aget_linked_person()
                if person:
                    assert person.fb_entity_id is not None

            if event.details.linked_entities.organization:
                org = await event.aget_linked_organization()
                if org:
                    assert org.fb_entity_id is not None

    finally:
        # Close client
        client.close()
