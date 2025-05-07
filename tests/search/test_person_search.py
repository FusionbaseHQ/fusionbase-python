"""Tests for Person search functionality."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.search.person_search import PersonSearchParams


class TestPersonSearch(unittest.TestCase):
    """Test cases for Person search functionality."""

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

    def test_person_search_by_name(self):
        """Test searching for persons by name."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search for a common name
        params = PersonSearchParams(q="Müller")
        results = self.client.search.persons.search(params)

        # Verify we have results
        self.assertGreater(len(results.items), 0)

        # Check the first result is a LazyReference to Person
        first_result = results.items[0]
        self.assertIsInstance(first_result, LazyReference)

        # Verify it has basic person properties via the LazyReference mechanism
        self.assertIsNotNone(first_result.entity_id)

        # Load the entity and check specific properties
        person = first_result.get()
        self.assertIsNotNone(person.given_name)


@pytest.mark.asyncio
async def test_person_search_async():
    """Test async person searching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        # Search for a person asynchronously
        params = PersonSearchParams(q="Patrick")

        try:
            # Use async methods directly on the client
            results = await client.search.persons.asearch(params)

            # Verify we have results
            assert len(results.items) > 0

            # Check first result
            person_ref = results.items[0]
            assert person_ref.entity_id is not None

            # Load the person to check properties
            person = await person_ref.aget()
            assert person.given_name is not None

        except Exception as e:
            pytest.fail(f"Async search failed with error: {e}")

    finally:
        # Close client (close both sync and async resources)
        await client.aclose()
        client.close()
