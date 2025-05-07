"""Tests for entity relation listing functionality."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.relation import Relation


class TestEntityRelations(unittest.TestCase):
    """Test cases for entity relation listing."""

    def setUp(self):
        """Set up test fixtures."""
        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Set up test entities
        self.location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich
        self.organization_id = "82a68ab9f7151fa0af9bf189c1caa753"  # OroraTech GmbH
        self.person_id = "2fde1bea7758bd19ddf4f00d8dc65497"  # Test person

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_list_location_relations(self):
        """Test listing relations for Location entity type."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Test class method to list relations
        relations = Location.list_relations(self.client)

        # Verify results
        self.assertIsInstance(relations, list)
        self.assertGreater(len(relations), 0)

        # Check that we got Relation objects
        for relation in relations:
            self.assertIsInstance(relation, Relation)
            self.assertEqual(relation.model_from.value, "location")

        # Test some expected relations for locations
        relation_labels = [r.label for r in relations]
        self.assertTrue(
            any(label.endswith("__DASHBOARD") for label in relation_labels))

    def test_entity_instance_get_relations(self):
        """Test getting relations from an entity instance."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Fetch a location entity
        location = self.client.entities.locations.from_id(self.location_id)

        # Get relations for this entity
        relations = location.get_relations(self.client)

        # Verify results
        self.assertIsInstance(relations, list)
        self.assertGreater(len(relations), 0)

        # Check that we got Relation objects
        for relation in relations:
            self.assertIsInstance(relation, Relation)
            self.assertEqual(relation.model_from.value, "location")

    def test_organization_relations(self):
        """Test listing relations for Organization entity type."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Test class method
        relations = Organization.list_relations(self.client)

        # Verify results
        self.assertIsInstance(relations, list)
        self.assertGreater(len(relations), 0)

        # Check relation objects
        for relation in relations:
            self.assertIsInstance(relation, Relation)
            self.assertEqual(relation.model_from.value, "organization")


@pytest.mark.asyncio
async def test_async_list_relations():
    """Test async relation listing for entity types."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Test async class method
        location_relations = await Location.alist_relations(client)

        # Verify results
        assert isinstance(location_relations, list)
        assert len(location_relations) > 0

        # Check relation objects
        for relation in location_relations:
            assert isinstance(relation, Relation)
            assert relation.model_from.value == "location"

        # Test with organization
        org_relations = await Organization.alist_relations(client)

        # Should have different relations than location
        assert set([r.label for r in org_relations
                   ]) != set([r.label for r in location_relations])

    finally:
        # Close client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_async_entity_instance_get_relations():
    """Test async relation getting from entity instance."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client
    client = Fusionbase(api_key=api_key)

    # Location ID for testing
    location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich

    try:
        # Get entity instance
        location = await client.entities.locations.afrom_id(location_id)

        # Get relations async
        relations = await location.aget_relations(client)

        # Verify results
        assert isinstance(relations, list)
        assert len(relations) > 0

        # Check relation objects
        for relation in relations:
            assert isinstance(relation, Relation)
            assert relation.model_from.value == "location"

    finally:
        # Close client
        await client.aclose()
        client.close()
