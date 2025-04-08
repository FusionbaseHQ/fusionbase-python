"""Tests for the Relation entity."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.feature import Feature
from fusionbase.entities.organization import Organization
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.types.entities import EntityType


class TestRelation(unittest.TestCase):
    """Test cases for Relation entity."""

    def setUp(self):
        """Set up test fixtures."""
        # Use a real relation ID that exists in your system
        self.valid_relation_id = "3138484719"  # Network relation
        self.invalid_relation_id = "non_existing_id"

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

    def test_relation_from_id_real_api(self):
        """Test getting relation data from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Get relation using real API
            relation = client.entities.relations.from_id(self.valid_relation_id)

            # Assert only non-dynamic fields
            self.assertEqual(relation.relation_id, self.valid_relation_id)
            self.assertEqual(relation.label, "ENTITY_NETWORK")

            # Check that model_from is properly converted to EntityType
            self.assertIsInstance(relation.model_from, EntityType)
            self.assertEqual(relation.model_from, EntityType.ORGANIZATION)

            # Check that model_to is properly converted to EntityType
            self.assertIsInstance(relation.model_to, EntityType)
            self.assertEqual(relation.model_to, EntityType.FEATURE)

            # Test helper properties
            self.assertEqual(relation.relation_name, "Network")

            if relation.description:
                self.assertIsInstance(relation.relation_description, str)

            # Test entity class methods
            from_class = relation.get_from_entity_class()
            to_class = relation.get_to_entity_class()

            self.assertEqual(from_class, Organization)
            self.assertEqual(to_class, Feature)

            # Verify that metadata exists
            self.assertTrue("created_at" in relation.metadata)
            self.assertTrue("updated_at" in relation.metadata)

        finally:
            # Always close the client
            client.close()

    def test_not_found_error_real_api(self):
        """Test real API error handling for non-existent relation."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        client = Fusionbase(api_key=self.api_key)

        try:
            # Test that the correct exception is raised
            with self.assertRaises(ResourceNotFoundError) as context:
                client.entities.relations.from_id(self.invalid_relation_id)

            # Verify exception details
            error = context.exception
            self.assertEqual(error.resource_type, "relation")
            self.assertEqual(error.resource_id, self.invalid_relation_id)
            self.assertEqual(error.status_code, 404)

        finally:
            # Always close the client
            client.close()


@pytest.mark.asyncio
async def test_relation_async_real_api():
    """Test async relation fetching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    valid_relation_id = "3138484719"  # Network relation

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Fetch async
        relation = await client.entities.relations.afrom_id(valid_relation_id)

        # Verify key fields but not dynamic ones
        assert relation.relation_id == valid_relation_id

        # Check that model_from/to fields are properly converted
        assert relation.model_from == EntityType.ORGANIZATION
        assert relation.model_to == EntityType.FEATURE

        # Check entity class methods
        from_class = relation.get_from_entity_class()
        to_class = relation.get_to_entity_class()

        assert from_class == Organization
        assert to_class == Feature

        assert relation.label == "ENTITY_NETWORK"

        # Just check that metadata exists
        assert "created_at" in relation.metadata
        assert "updated_at" in relation.metadata

    finally:
        # Close client
        client.close()
