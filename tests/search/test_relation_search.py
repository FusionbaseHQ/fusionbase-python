"""Tests for Relation search functionality."""

import traceback
import unittest

import pytest
from conftest import get_api_key

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.relation import Relation


class TestRelationSearch(unittest.TestCase):
    """Test cases for Relation search functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Use API key from environment or skip tests if not available
        self.api_key = get_api_key()
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_relation_search_by_query(self):
        """Test searching for relations by query."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search for a relation - q is now a required positional argument
        results = self.client.search.relations.search("network")

        # Verify we have results
        self.assertGreater(len(results.items), 0)

        # Check the first result is a LazyReference to Relation
        first_result = results.items[0]
        self.assertIsInstance(first_result, LazyReference)

        # Verify it has the entity_id property from LazyReference
        self.assertIsNotNone(first_result.entity_id)

        # Load the entity and check specific properties
        relation = first_result.get()
        self.assertIsInstance(relation, Relation)
        self.assertIsNotNone(relation.relation_name)
        self.assertIsNotNone(relation.model_from)
        self.assertIsNotNone(relation.model_to)

    def test_relation_search_with_limit(self):
        """Test searching for relations with limit."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search with a limit of 2
        results = self.client.search.relations.search("network", limit=2)

        # Verify result count respects the limit
        self.assertLessEqual(len(results.items), 2)

    def test_search_param_kwargs(self):
        """Test that search accepts kwargs directly."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search using positional q and kwargs for other params
        results = self.client.search.relations.search("network", limit=3)

        # Verify results
        self.assertIsNotNone(results)
        self.assertLessEqual(len(results.items), 3)

    def test_relation_search_with_missing_required_params(self):
        """Test that search fails when required parameters are missing."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Try to search with empty query
        with self.assertRaises(ValueError):
            # Empty query should raise ValueError
            self.client.search.relations.search("")


@pytest.mark.asyncio
async def test_relation_search_async():
    """Test async relation searching with real API."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        try:
            # Use direct async method on search manager
            results = await client.search.relations.asearch("network")

            # Verify we have results
            assert len(results.items) > 0

            # Check first result is a LazyReference
            relation_ref = results.items[0]
            assert isinstance(relation_ref, LazyReference)
            assert relation_ref.entity_id is not None

            try:
                # Explicitly load the relation with better error reporting
                relation = await relation_ref.aget()
                assert isinstance(relation, Relation)
                assert relation.relation_name is not None
                assert relation.model_from is not None
                assert relation.model_to is not None
            except Exception as load_error:
                print(f"Error loading relation: {load_error}")
                traceback.print_exc()
                pytest.fail(f"Failed to load relation: {load_error}")

        except Exception as e:
            print(f"Async search error: {e}")
            traceback.print_exc()
            pytest.fail(f"Async search failed with error: {e}")

    finally:
        # Close client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_relation_search_async_with_limit():
    """Test async relation searching with limit parameter."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Search with limit parameter
        results = await client.search.relations.asearch("network", limit=2)

        # Verify limit is respected
        assert len(results.items) <= 2

    finally:
        # Close client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_relation_search_async_with_missing_required_params():
    """Test that async search fails when required parameters are missing."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Try to search with empty query
        with pytest.raises(ValueError):
            # Empty query should raise ValueError
            await client.search.relations.asearch("")

    finally:
        # Close client
        await client.aclose()
        client.close()
