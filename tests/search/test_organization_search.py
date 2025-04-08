"""Tests for Organization search functionality."""

import os
from typing import Any, Dict
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.organization import Organization
from fusionbase.search.organization_search import OrganizationSearchParams
from fusionbase.types.entities import FilterKey


class TestOrganizationSearch(unittest.TestCase):
    """Test cases for Organization search functionality."""

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

    def test_organization_search_by_query(self):
        """Test searching for organizations by query."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search for an organization
        params = OrganizationSearchParams(q="GmbH")
        results = self.client.search.organizations.search(params)

        # Verify we have results
        self.assertGreater(len(results.items), 0)

        # Check the first result is a LazyReference to Organization
        first_result = results.items[0]
        self.assertIsInstance(first_result, LazyReference)

        # Verify it has the entity_id property from LazyReference
        self.assertIsNotNone(first_result.entity_id)

        # Load the entity and check specific properties
        organization = first_result.get()
        self.assertIsInstance(organization, Organization)
        self.assertIsNotNone(organization.name)

    def test_organization_search_with_postal_code(self):
        """Test searching for organizations with postal code."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search with postal code filter using FilterKey enum
        # TODO: Add more comprehensive tests for all available filters
        params = OrganizationSearchParams(
            q="GmbH", filters={FilterKey.POSTAL_CODE: "80992"})
        results = self.client.search.organizations.search(params)

        # We don't assert on result count as it depends on the data
        self.assertIsNotNone(results)

    def test_organization_search_with_invalid_filter_key(self):
        """Test that search fails with invalid filter key."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Test with invalid filter key
        with self.assertRaises(ValueError):
            # This should fail because we're using a string that's not a valid FilterKey
            params = OrganizationSearchParams(
                q="GmbH",
                filters={"invalid_key": "some_value"
                        }  # This should cause validation to fail
            )

    def test_search_param_kwargs(self):
        """Test that search accepts kwargs directly."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search using kwargs directly
        results = self.client.search.organizations.search(q="Tech", limit=5)

        # Verify results
        self.assertIsNotNone(results)
        self.assertLessEqual(len(results.items),
                             5)  # Should respect limit param

    def test_organization_search_with_missing_required_params(self):
        """Test that search fails when required parameters are missing."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Try to search without required query
        with self.assertRaises(ValueError):
            # We need to initialize OrganizationSearchParams with default empty
            # data to properly test validation
            params = OrganizationSearchParams(q="")
            self.client.search.organizations.search(params)


@pytest.mark.asyncio
async def test_organization_search_async():
    """Test async organization searching with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key - no need for separate async client
    client = Fusionbase(api_key=api_key)

    try:
        # Search for organizations asynchronously
        params = OrganizationSearchParams(q="OroraTech")

        # Use direct async method on the search manager
        results = await client.search.organizations.asearch(params)

        # Verify we have results
        assert len(results.items) > 0

        # Check first result is a LazyReference
        org_ref = results.items[0]
        assert isinstance(org_ref, LazyReference)
        assert org_ref.entity_id is not None

        # Load the organization
        organization = await org_ref.aget()
        assert isinstance(organization, Organization)
        assert organization.name is not None

    finally:
        # Close client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_organization_search_async_with_postal_code():
    """Test async organization searching with postal code."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key - no need for separate async client
    client = Fusionbase(api_key=api_key)

    try:
        # Search with postal code filter
        params = OrganizationSearchParams(
            q="GmbH", filters={FilterKey.POSTAL_CODE: "80992"})
        results = await client.search.organizations.asearch(params)

        # We don't assert on result count as it depends on the data
        assert results is not None

    finally:
        # Close client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_organization_search_async_with_missing_required_params():
    """Test that async search fails when required parameters are missing."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key - no need for separate async client
    client = Fusionbase(api_key=api_key)

    try:
        # Try to search without required query
        with pytest.raises(ValueError):
            # Empty query should raise ValueError
            await client.search.organizations.asearch(q="")

    finally:
        # Close client
        await client.aclose()
        client.close()
