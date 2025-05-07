"""Tests for the Organization entity."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.types.entities import OrganizationStatus
from fusionbase.types.entities import OrganizationSubtype


class TestOrganization(unittest.TestCase):
    """Test cases for Organization entity."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_organization_id = "82a68ab9f7151fa0af9bf189c1caa753"  # OroraTech GmbH
        self.invalid_organization_id = "non_existing_id"

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

    def test_organization_from_id_real_api(self):
        """Test getting organization data from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Get organization using real API
            organization = client.entities.organizations.from_id(
                self.valid_organization_id)

            # Assert only non-dynamic fields
            self.assertEqual(organization.fb_entity_id,
                             self.valid_organization_id)
            self.assertEqual(organization.name, "OroraTech GmbH")
            self.assertEqual(organization.entity_subtype,
                             OrganizationSubtype.CORPORATION)

            # Test status information
            self.assertTrue(organization.is_active)
            self.assertEqual(organization.status.status,
                             OrganizationStatus.ACTIVE)

            # Test contact properties if available
            if organization.contact and organization.contact.websites:
                self.assertEqual(organization.primary_website,
                                 organization.contact.websites.primary)

            if organization.contact and organization.contact.phone_numbers:
                self.assertEqual(organization.primary_phone,
                                 organization.contact.phone_numbers.primary)

            # Test address if available
            if organization.address:
                self.assertIsNotNone(organization.address.formatted_address)
                if organization.address.coordinate:
                    self.assertIsNotNone(
                        organization.address.coordinate.latitude)
                    self.assertIsNotNone(
                        organization.address.coordinate.longitude)

            # Verify that dynamic fields exist but don't assert their values
            self.assertIsNotNone(organization.fb_entity_version)

        finally:
            # Always close the client
            client.close()

    def test_not_found_error_real_api(self):
        """Test real API error handling for non-existent organization."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Test that the correct exception is raised
            with self.assertRaises(ResourceNotFoundError) as context:
                client.entities.organizations.from_id(
                    self.invalid_organization_id)

            # Verify exception details
            error = context.exception
            self.assertEqual(error.resource_type, "organization")
            self.assertEqual(error.resource_id, self.invalid_organization_id)
            self.assertEqual(error.status_code, 404)

        finally:
            # Always close the client
            client.close()


@pytest.mark.asyncio
async def test_organization_async_real_api():
    """Test async organization fetching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    valid_organization_id = "82a68ab9f7151fa0af9bf189c1caa753"  # OroraTech GmbH

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Fetch async
        organization = await client.entities.organizations.afrom_id(
            valid_organization_id)

        # Verify key fields but not dynamic ones
        assert organization.fb_entity_id == valid_organization_id
        assert organization.name == "OroraTech GmbH"
        assert organization.entity_subtype == OrganizationSubtype.CORPORATION
        assert organization.is_active

        # Just check dynamic fields exist
        assert organization.fb_entity_version is not None

    finally:
        # Close client
        client.close()
