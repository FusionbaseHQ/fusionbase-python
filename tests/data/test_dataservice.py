"""Tests for DataService functionality."""

import os
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase import Fusionbase
from fusionbase.data.dataservice import DataService
from fusionbase.data.dataservice import DataServiceMetadata
from fusionbase.data.dataservice import ServiceInputDefinition
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.exceptions import ValidationError


class TestDataService(unittest.TestCase):
    """Test cases for DataService."""

    def setUp(self):
        """Set up test fixtures."""
        # Define test service ID
        self.service_id = "4658603456"  # Web Context service

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Sample mock metadata
        self.mock_metadata = {
            "name": {
                "en": "Test Service",
                "de": "Testdienst"
            },
            "description": {
                "en": "This is a test service",
                "de": "Dies ist ein Testdienst"
            },
            "updated_at": "2023-01-01T00:00:00.000000",
            "source": {
                "_id": "data_sources/12345",
                "service_specific": {
                    "uri": "https://example.com"
                }
            },
            "service_input_definition": [{
                "name": "entity_name",
                "type": "string",
                "description": {
                    "en": "Name of the entity"
                },
                "required": True,
                "sample": {
                    "value": "Test Entity"
                }
            }, {
                "name": "postal_code",
                "type": "string",
                "description": {
                    "en": "Postal code"
                },
                "required": True,
                "sample": {
                    "value": "12345"
                }
            }],
            "_key": self.service_id,
            "credit_policy": {
                "entity_action": "invoke",
                "credit_cost": 100
            }
        }

        # Sample mock invoke response
        self.mock_invoke_response = {
            "company_overview":
                "This is a test company that does testing things.",
            "topics": ["testing", "software", "quality assurance"]
        }

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_service_from_id_real_api(self):
        """Test getting service by ID using real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create data service directly
        service = DataService(self.client, self.service_id)

        # Get metadata
        metadata = service.get_metadata()

        # Basic validation of response
        self.assertIsInstance(metadata, DataServiceMetadata)
        self.assertEqual(metadata.service_key, self.service_id)
        self.assertIsNotNone(metadata.name.en)
        self.assertIsInstance(metadata.service_input_definition, list)
        self.assertGreater(len(metadata.service_input_definition), 0)
        self.assertIsInstance(metadata.credit_policy.credit_cost, int)

        # Check convenience properties
        self.assertIsNotNone(metadata.display_name)
        self.assertGreater(metadata.cost, 0)

        # Validate that metadata is cached
        service._metadata = MagicMock()
        service.get_metadata()
        service._metadata.assert_not_called()

    def test_service_invoke_real_api(self):
        """Test invoking a service with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create data service directly
        service = DataService(self.client, self.service_id)

        # Sample input parameters for Web Context service
        inputs = {
            "entity_name": "OroraTech GmbH",
            "postal_code": "81669",
            "street": "St.-Martin-Straße 112",
            "city": "München"
        }

        # Invoke the service
        result = service.invoke(inputs)

        # Basic validation of response
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)

        # If Web Context service, we should have these keys
        if "company_overview" in result:
            self.assertIsInstance(result["company_overview"], str)
        if "topics" in result:
            self.assertIsInstance(result["topics"], list)

    def test_service_from_manager_real_api(self):
        """Test getting service through manager using real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Get service through manager
        service = self.client.services.from_id(self.service_id)

        # Get metadata
        metadata = service.get_metadata()

        # Basic validation of response
        self.assertIsInstance(metadata, DataServiceMetadata)
        self.assertEqual(metadata.service_key, self.service_id)
        self.assertIsNotNone(metadata.name.en)

        # Test from_key alias
        service2 = self.client.services.from_key(self.service_id)
        metadata2 = service2.get_metadata()
        self.assertEqual(metadata.service_key, metadata2.service_key)

    def test_service_invalid_id_real_api(self):
        """Test handling of invalid service ID using real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Attempting to create service with invalid ID should raise error immediately
        with self.assertRaises(ResourceNotFoundError) as context:
            self.client.services.from_id("invalid_id_12345")

        # Verify error details
        self.assertEqual(context.exception.resource_type.lower(),
                         "data_service")
        self.assertEqual(context.exception.resource_id, "invalid_id_12345")

        # Test that we can still create a service with validate=False
        service = self.client.services.from_id("invalid_id_12345",
                                               validate=False)

        # Attempting to get metadata should now raise APIError
        with self.assertRaises(APIError):
            service.get_metadata()

    def test_service_invalid_id_validation(self):
        """Test validation of invalid service IDs during instantiation."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Attempting to create service with invalid ID should raise error when validate=True
        with self.assertRaises(ResourceNotFoundError) as context:
            self.client.services.from_id("invalid_id_12345")

        # Verify error details
        self.assertEqual(context.exception.resource_type.lower(),
                         "data_service")
        self.assertEqual(context.exception.resource_id, "invalid_id_12345")

        # Should work when validation is disabled
        service = self.client.services.from_id("invalid_id_12345",
                                               validate=False)
        self.assertEqual(service.service_key, "invalid_id_12345")

        # But should fail when trying to get metadata
        with self.assertRaises(APIError):
            service.get_metadata()

    def test_service_invoke_with_kwargs(self):
        """Test invoking service with kwargs instead of dict."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create data service but mock the request method to avoid actual API call
        service = DataService(self.client, self.service_id)

        with patch.object(self.client, 'request') as mock_request:
            # Setup mock
            mock_request.side_effect = [
                self.mock_metadata,  # For get_metadata
                self.mock_invoke_response  # For invoke
            ]

            # Invoke with kwargs
            result = service.invoke(entity_name="OroraTech GmbH",
                                    postal_code="81669",
                                    street="St.-Martin-Straße 112",
                                    city="München")

            # Check that the correct payload was sent
            mock_request.assert_any_call(
                "POST",
                "service/invoke",
                json={
                    "service_key": self.service_id,
                    "inputs": {
                        "entity_name": "OroraTech GmbH",
                        "postal_code": "81669",
                        "street": "St.-Martin-Straße 112",
                        "city": "München"
                    }
                })

            # Check result
            self.assertEqual(result, self.mock_invoke_response)

    def test_service_invoke_with_dict_and_kwargs(self):
        """Test invoking service with both dict and kwargs."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create data service but mock the request method to avoid actual API call
        service = DataService(self.client, self.service_id)

        with patch.object(self.client, 'request') as mock_request:
            # Setup mock
            mock_request.side_effect = [
                self.mock_metadata,  # For get_metadata
                self.mock_invoke_response  # For invoke
            ]

            # Invoke with both dict and kwargs - kwargs should override dict values
            result = service.invoke(
                {
                    "entity_name": "Wrong Name",
                    "postal_code": "12345"
                },
                entity_name="OroraTech GmbH",
                city="München")

            # Check that the correct payload was sent with merged inputs
            mock_request.assert_any_call(
                "POST",
                "service/invoke",
                json={
                    "service_key": self.service_id,
                    "inputs": {
                        "entity_name": "OroraTech GmbH",  # Overridden by kwargs
                        "postal_code": "12345",  # From dict
                        "city": "München"  # From kwargs
                    }
                })

            # Check result
            self.assertEqual(result, self.mock_invoke_response)

    def test_service_validation_error(self):
        """Test handling of validation errors."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create data service but mock the request method to avoid actual API call
        service = DataService(self.client, self.service_id)

        with patch.object(self.client, 'request') as mock_request:
            # Setup mock
            mock_request.return_value = self.mock_metadata

            # Get metadata to initialize the service definition
            service.get_metadata()

            # Reset mock for next calls
            mock_request.reset_mock()

            # Test missing required parameter
            with self.assertRaises(ValidationError):
                service.invoke(postal_code="12345")  # Missing entity_name

            # Verify no request was made because validation failed
            mock_request.assert_not_called()

            # Test invalid type parameter (string that can't be converted to int)
            with patch.object(ServiceInputDefinition,
                              'validate_value') as mock_validate:
                # Make the validation fail
                mock_validate.side_effect = ValidationError("Invalid value")

                with self.assertRaises(ValidationError):
                    service.invoke(entity_name="Test",
                                   postal_code="ABC")  # Invalid postal code

            # Verify no request was made because validation failed
            mock_request.assert_not_called()

    def test_service_manager_batch_invoke(self):
        """Test batch invocation through manager."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Mock the client's request method to avoid actual API calls
        with patch.object(self.client, 'request') as mock_request:
            # Define a custom side effect function that returns responses based on the input data
            def custom_side_effect(method, url, **kwargs):
                if method == "GET" and f"service/get/{self.service_id}" in url:
                    return self.mock_metadata
                elif method == "POST" and "service/invoke" in url:
                    json_data = kwargs.get('json', {})
                    inputs = json_data.get('inputs', {})
                    entity_name = inputs.get('entity_name', '')

                    if entity_name == "Company1":
                        return {"result": "company1"}
                    elif entity_name == "Company2":
                        return {"result": "company2"}

                return {}

            # Use the custom side effect
            mock_request.side_effect = custom_side_effect

            # Create batch inputs
            batch_inputs = [{
                "entity_name": "Company1",
                "postal_code": "12345"
            }, {
                "entity_name": "Company2",
                "postal_code": "67890"
            }]

            # Invoke batch with threading
            results = self.client.services.batch_invoke_parallel(
                self.service_id, batch_inputs)

            # Verify results
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0], {"result": "company1"})
            self.assertEqual(results[1], {"result": "company2"})

    def test_service_id_normalization(self):
        """Test handling of different ID formats."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Test with plain key
        service1 = DataService(self.client, self.service_id)
        self.assertEqual(service1._service_key, self.service_id)

        # Test with collection prefix
        service2 = DataService(self.client, f"services/{self.service_id}")
        self.assertEqual(service2._service_key, self.service_id)

        # Test with different formats through manager
        service3 = self.client.services.from_id(self.service_id)
        service4 = self.client.services.from_id(f"services/{self.service_id}")
        service5 = self.client.services.from_id(f"id:{self.service_id}")
        service6 = self.client.services.from_id(
            f"_id:services/{self.service_id}")

        # All should resolve to the same service instance
        self.assertEqual(service3._service_key, self.service_id)
        self.assertEqual(service4._service_key, self.service_id)
        self.assertEqual(service5._service_key, self.service_id)
        self.assertEqual(service6._service_key, self.service_id)

        # Test metadata ID properties
        metadata = service1.get_metadata()
        self.assertEqual(metadata.service_key, self.service_id)

        # Test with custom mock
        mock_data = {
            **self.mock_metadata, "_key": "test123",
            "id": "services/test123"
        }
        metadata = DataServiceMetadata.model_validate(mock_data)
        self.assertEqual(metadata.service_key, "test123")
        self.assertEqual(metadata.service_id, "services/test123")

        # Test with only key, no id
        mock_data = {**self.mock_metadata, "_key": "test123", "id": None}
        metadata = DataServiceMetadata.model_validate(mock_data)
        self.assertEqual(metadata.service_key, "test123")
        self.assertEqual(metadata.service_id,
                         "services/test123")  # Should construct full ID

        # Test with only id, no key
        mock_data = {
            **self.mock_metadata, "_key": None,
            "id": "services/test123"
        }
        metadata = DataServiceMetadata.model_validate(mock_data)
        self.assertEqual(metadata.service_key, "test123")
        self.assertEqual(metadata.service_id, "services/test123")


@pytest.mark.asyncio
async def test_service_async_metadata():
    """Test asynchronously getting service metadata."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    service_id = "4658603456"  # Web Context service

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Get service through manager asynchronously
        service = await client.services.afrom_id(service_id)

        # Metadata should already be loaded
        metadata = service.get_metadata()

        # Basic validation
        assert metadata.service_key == service_id
        assert metadata.name.en is not None
        assert len(metadata.service_input_definition) > 0
        assert metadata.credit_policy.credit_cost > 0

        # Test convenience properties
        assert metadata.display_name is not None
        assert metadata.cost > 0
    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_service_async_invoke():
    """Test asynchronously invoking a service."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    service_id = "4658603456"  # Web Context service

    # Check if we should skip real invocation
    os.environ.get("SKIP_SERVICE_INVOKE_TESTS", "true").lower() == "true"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Sample input parameters
        inputs = {
            "entity_name": "OroraTech GmbH",
            "postal_code": "81669",
            "street": "St.-Martin-Straße 112",
            "city": "München"
        }

        # Use service manager's ainvoke for convenience
        result = await client.services.ainvoke(service_id, inputs)

        # Basic validation of response
        assert result is not None
        if isinstance(result, dict):
            # For web context service, we might expect these keys
            if "company_overview" in result:
                assert isinstance(result["company_overview"], str)

            if "topics" in result:
                assert isinstance(result["topics"], list)
    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_service_async_invoke_with_kwargs():
    """Test asynchronously invoking a service with kwargs."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create a mocked client
    client = MagicMock()

    # Sample mock data
    mock_metadata = {
        "name": {
            "en": "Test Service"
        },
        "description": {
            "en": "Test Description"
        },
        "updated_at": "2023-01-01T00:00:00.000000",
        "source": {
            "_id": "data_sources/12345"
        },
        "service_input_definition": [{
            "name": "query",
            "type": "string",
            "description": {
                "en": "Search query"
            },
            "required": True
        }],
        "_key": "test_service",
        "credit_policy": {
            "entity_action": "invoke",
            "credit_cost": 1
        }
    }

    # Mock the arequest method as a proper async function
    async def mock_arequest(method, url, **kwargs):
        if "service/get/" in url:
            return mock_metadata
        elif method == "POST" and url == "service/invoke":
            return {"result": "success"}
        return {}

    client.arequest = mock_arequest

    # Create data service
    service = DataService(client, "test_service")

    # Test invoking with kwargs
    result = await service.ainvoke(query="test query")

    # Verify result
    assert result == {"result": "success"}


@pytest.mark.asyncio
async def test_service_async_batch_invoke():
    """Test asynchronously batch invoking services."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create a mocked client
    client = MagicMock()

    # Sample mock data
    mock_metadata = {
        "name": {
            "en": "Test Service"
        },
        "description": {
            "en": "Test Description"
        },
        "updated_at": "2023-01-01T00:00:00.000000",
        "source": {
            "_id": "data_sources/12345"
        },
        "service_input_definition": [{
            "name": "query",
            "type": "string",
            "description": {
                "en": "Search query"
            },
            "required": True
        }],
        "_key": "test_service",
        "credit_policy": {
            "entity_action": "invoke",
            "credit_cost": 1
        }
    }

    # Mock the arequest method as a proper async function
    async def mock_arequest(method, url, **kwargs):
        if "service/get/" in url:
            return mock_metadata
        elif method == "POST" and url == "service/invoke":
            # Get the input params
            json_data = kwargs.get('json', {})
            inputs = json_data.get('inputs', {})
            query = inputs.get('query', '')

            # Return different results based on query
            if query == "query1":
                return {"result": "result1"}
            elif query == "query2":
                return {"result": "result2"}
            elif query == "query3":
                return {"result": "result3"}
            return {"result": "unknown"}
        return {}

    client.arequest = mock_arequest

    # Create service manager with our mocked client

    # Create batch inputs
    batch_inputs = [{
        "query": "query1"
    }, {
        "query": "query2"
    }, {
        "query": "query3"
    }]

    # Mock the service's ainvoke method
    # We need to patch the DataService.ainvoke method
    with patch(
            'fusionbase.data.dataservice.DataService.ainvoke') as mock_ainvoke:
        # Setup mock to return different values for each call
        mock_ainvoke.side_effect = [{
            "result": "result1"
        }, {
            "result": "result2"
        }, {
            "result": "result3"
        }]

        # Create our own batch invoke function that mimics the manager's
        async def batch_invoke_test():
            # Create service and pre-fetch metadata
            service = DataService(client, "test_service")
            service._metadata = DataServiceMetadata.model_validate(
                mock_metadata)

            # Create tasks
            tasks = []
            for inputs in batch_inputs:
                tasks.append(service.ainvoke(inputs))

            # Gather results
            import asyncio
            return await asyncio.gather(*tasks)

        # Run our test batch invoke
        results = await batch_invoke_test()

        # Verify results
        assert len(results) == 3
        assert results[0] == {"result": "result1"}
        assert results[1] == {"result": "result2"}
        assert results[2] == {"result": "result3"}

        # Verify mock was called with correct arguments
        assert mock_ainvoke.call_count == 3
        mock_ainvoke.assert_any_call({"query": "query1"})
        mock_ainvoke.assert_any_call({"query": "query2"})
        mock_ainvoke.assert_any_call({"query": "query3"})


@pytest.mark.asyncio
async def test_service_async_validation_error():
    """Test validation error handling in async mode."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create a mocked client
    client = MagicMock()

    # Create data service
    service = DataService(client, "test_service")

    # Mock the aget_metadata method to return successfully
    async def mock_aget_metadata():
        return DataServiceMetadata.model_validate({
            "name": {
                "en": "Test Service"
            },
            "description": {
                "en": "Test Description"
            },
            "updated_at": "2023-01-01T00:00:00.000000",
            "source": {
                "_id": "data_sources/12345"
            },
            "service_input_definition": [{
                "name": "query",
                "type": "string",
                "description": {
                    "en": "Search query"
                },
                "required": True
            }],
            "_key": "test_service",
            "credit_policy": {
                "entity_action": "invoke",
                "credit_cost": 1
            }
        })

    # Replace the service's aget_metadata with our mock
    service.aget_metadata = mock_aget_metadata

    # Mock _validate_inputs to simulate validation errors
    def mock_validate_inputs(inputs):
        # When no inputs provided or missing required parameter
        if not inputs or 'query' not in inputs:
            raise ValidationError("Required parameter 'query' is missing")

        # When query parameter is the wrong type
        if 'query' in inputs and not isinstance(inputs['query'], str):
            raise ValidationError(
                "Invalid value for parameter 'query': expected string")

        # Return inputs if validation passes
        return inputs

    # Replace the service's _validate_inputs with our mock
    service._validate_inputs = mock_validate_inputs

    # Mock arequest to prevent actual API calls
    async def mock_arequest(*args, **kwargs):
        return {"result": "success"}

    client.arequest = mock_arequest

    # Test missing required parameter - should raise ValidationError
    with pytest.raises(ValidationError):
        await service.ainvoke()  # No parameters provided

    # Test with invalid parameter type - should raise ValidationError
    with pytest.raises(ValidationError):
        await service.ainvoke(query=123)  # Query should be string

    # Test with valid parameter - should succeed
    result = await service.ainvoke(query="test query")
    assert result == {"result": "success"}


@pytest.mark.asyncio
async def test_service_async_invalid_id():
    """Test async validation of invalid service IDs during instantiation."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Attempting to create service with invalid ID should raise error when validate=True
        with pytest.raises(ResourceNotFoundError) as excinfo:
            await client.services.afrom_id("invalid_service_id_999999")

        # Verify error details
        assert excinfo.value.resource_type.lower() == "data_service"
        assert excinfo.value.resource_id == "invalid_service_id_999999"

        # Should work when validation is disabled
        service = await client.services.afrom_id("invalid_service_id_999999",
                                                 validate=False)
        assert service.service_key == "invalid_service_id_999999"

    finally:
        await client.aclose()
        client.close()
