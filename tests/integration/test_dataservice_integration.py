"""Integration tests for DataService functionality."""

import os
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.data.dataservice import DataService
from fusionbase.data.dataservice import DataServiceMetadata
from fusionbase.managers.dataservice_manager import DataServiceManager


class TestDataServiceIntegration(unittest.TestCase):
    """Integration tests for DataService with the Fusionbase client."""

    def setUp(self):
        """Set up test fixtures."""
        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Test service ID
        self.service_id = "4658603456"  # Web Context service

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_client_has_services_manager(self):
        """Test that the client has a services manager."""
        self.assertIsInstance(self.client.services, DataServiceManager)

    def test_services_manager_methods(self):
        """Test the methods of the services manager."""
        # Test from_id method
        service = self.client.services.from_id(self.service_id)
        self.assertIsInstance(service, DataService)
        self.assertEqual(service.service_id, self.service_id)

        # Test from_key alias
        service2 = self.client.services.from_key(self.service_id)
        self.assertEqual(service.service_id, service2.service_id)

        # Test normalize_service_id
        normalized = self.client.services._normalize_service_id("_id:12345")
        self.assertEqual(normalized, "12345")

        normalized = self.client.services._normalize_service_id("id:67890")
        self.assertEqual(normalized, "67890")

        normalized = self.client.services._normalize_service_id("plain_id")
        self.assertEqual(normalized, "plain_id")

    def test_manager_caching(self):
        """Test that the manager caches services."""
        # First access should create the service
        service1 = self.client.services.from_id(self.service_id)

        # Second access should return the same instance
        service2 = self.client.services.from_id(self.service_id)

        # They should be the exact same instance
        self.assertIs(service1, service2)

    def test_client_context_manager(self):
        """Test using the client as a context manager."""
        with Fusionbase(api_key=self.api_key) as client:
            # Check that services manager is available
            self.assertIsInstance(client.services, DataServiceManager)

            # Get a service
            service = client.services.from_id(self.service_id)
            self.assertIsInstance(service, DataService)

            # Get metadata
            metadata = service.get_metadata()
            self.assertIsInstance(metadata, DataServiceMetadata)
            self.assertEqual(metadata.service_key, self.service_id)


@pytest.mark.asyncio
async def test_client_async_context_manager():
    """Test using the client as an async context manager."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    service_id = "4658603456"  # Web Context service

    async with Fusionbase(api_key=api_key) as client:
        # Check that services manager is available
        assert isinstance(client.services, DataServiceManager)

        # Get a service asynchronously
        service = await client.services.afrom_id(service_id)
        assert isinstance(service, DataService)

        # Metadata should be pre-loaded
        metadata = service.get_metadata()
        assert isinstance(metadata, DataServiceMetadata)
        assert metadata.service_key == service_id


@pytest.mark.asyncio
async def test_batch_invoke_parallel_integration():
    """Test batch invoke parallel with real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    service_id = "4658603456"  # Web Context service

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create batch inputs - use different companies
        batch_inputs = [{
            "entity_name": "OroraTech GmbH",
            "postal_code": "81669",
            "street": "St.-Martin-Straße 112",
            "city": "München"
        }, {
            "entity_name": "Microsoft Deutschland GmbH",
            "postal_code": "80992",
            "street": "Walter-Gropius-Straße 5",
            "city": "München"
        }]

        # Execute batch invoke asynchronously
        results = await client.services.abatch_invoke_parallel(
            service_id, batch_inputs, max_concurrency=2)

        # Validate
        assert len(results) == 2

        # Basic structure validation for each result
        for i, result in enumerate(results):
            assert isinstance(result, dict)
            company = batch_inputs[i]["entity_name"]
            print(f"Result for {company}: {len(str(result))} bytes")

            # For web context service, we might expect these keys
            if "company_overview" in result:
                assert isinstance(result["company_overview"], str)
                # Company name should appear in the overview
                assert batch_inputs[i]["entity_name"] in result[
                    "company_overview"]

    finally:
        # Always close the client
        await client.aclose()
        client.close()
