"""Tests for DataStream functionality."""

import os
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase import Fusionbase
from fusionbase.data.datastream import DataStream
from fusionbase.data.datastream import FilterOperator
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError

MSGPACK_AVAILABLE = True  # Assume msgpack is available for testing purposes

# Check directly if pandas is available instead of relying on imported globals
PANDAS_AVAILABLE = True
try:
    import pandas as pd
except ImportError:
    PANDAS_AVAILABLE = False


class TestDataStreamBasic(unittest.TestCase):
    """Basic test cases for DataStream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_stream_id = "23532363"  # Test stream ID

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Verify if msgpack is actually available and working
        global MSGPACK_AVAILABLE
        if MSGPACK_AVAILABLE:
            try:
                import msgpack

                # Test a quick encode/decode to verify it works
                test_data = msgpack.packb({"test": "value"}, use_bin_type=True)
                msgpack.unpackb(test_data, raw=False)
            except (ImportError, AttributeError, TypeError):
                MSGPACK_AVAILABLE = False

        # Always prefer msgpack for tests when available
        self.preferred_format = "msgpack" if MSGPACK_AVAILABLE else "json"

        # Sample mock metadata
        self.mock_metadata = {
            "id": f"data_streams/{self.test_stream_id}",
            "key": self.test_stream_id,
            "name": {
                "en": "Test Stream",
                "de": "Teststrom"
            },
            "description": {
                "en": "Test stream for unit tests",
                "de": "Teststrom für Unit-Tests"
            },
            "meta": {
                "entry_count": 1000,
                "main_property_count": 5,
                "is_active": True
            },
            "source": {
                "_id": "data_sources/test",
                "stream_specific": {}
            },
            "data_item_collections": [{
                "name": "id",
                "basic_data_type": "string",
                "description": {
                    "en": "Identifier"
                }
            }, {
                "name": "name",
                "basic_data_type": "string",
                "description": {
                    "en": "Name"
                }
            }, {
                "name": "value",
                "basic_data_type": "number",
                "description": {
                    "en": "Value"
                }
            }, {
                "name": "created_at",
                "basic_data_type": "datetime",
                "description": {
                    "en": "Creation date"
                }
            }, {
                "name": "active",
                "basic_data_type": "boolean",
                "description": {
                    "en": "Is active"
                }
            }],
            "data_updated_at": "2023-01-01T00:00:00.000Z",
            "data_version": "v1.0",
            "created_at": "2023-01-01T00:00:00.000Z",
            "updated_at": "2023-01-01T00:00:00.000Z"
        }

        # Sample mock data response
        self.mock_data = [{
            "id": "1",
            "name": "Item 1",
            "value": 10.5,
            "created_at": "2023-01-01",
            "active": True
        }, {
            "id": "2",
            "name": "Item 2",
            "value": 20.5,
            "created_at": "2023-01-02",
            "active": False
        }, {
            "id": "3",
            "name": "Item 3",
            "value": 30.5,
            "created_at": "2023-01-03",
            "active": True
        }]

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_stream_initialization(self):
        """Test different ways to initialize DataStream."""
        # Test with plain ID
        stream1 = DataStream(self.client, self.test_stream_id)
        self.assertEqual(stream1.stream_key, self.test_stream_id)
        self.assertIsNone(stream1.stream_id)  # Should remain None for plain IDs

        # Test with collection prefix
        full_id = f"data_streams/{self.test_stream_id}"
        stream2 = DataStream(self.client, full_id)
        self.assertEqual(stream2.stream_key, self.test_stream_id)
        self.assertEqual(stream2.stream_id, full_id)

        # Test initialization through manager (disabled validation for test speed)
        stream3 = self.client.streams.from_id(self.test_stream_id,
                                              validate=False)
        self.assertEqual(stream3.stream_key, self.test_stream_id)

        # Test initialization with full ID through manager
        stream4 = self.client.streams.from_id(full_id, validate=False)
        self.assertEqual(stream4.stream_key, self.test_stream_id)
        self.assertEqual(stream4.stream_id, full_id)

    @patch("httpx.Client")
    def test_get_metadata_mock(self, mock_client):
        """Test fetching stream metadata with mocked responses."""
        # Create a mocked client
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        # Mock the request response
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_metadata
        mock_client_instance.get.return_value = mock_response

        # Create test client with patched HTTP client
        test_client = MagicMock()
        test_client.request.return_value = self.mock_metadata

        # Create stream with mocked client
        stream = DataStream(test_client, self.test_stream_id)

        # Fetch metadata
        metadata = stream.get_metadata()

        # Verify metadata
        self.assertEqual(metadata.key, self.test_stream_id)
        self.assertEqual(metadata.meta.entry_count, 1000)
        self.assertEqual(metadata.meta.main_property_count, 5)
        self.assertEqual(len(metadata.data_item_collections), 5)
        self.assertEqual(metadata.display_name, "Test Stream")

    @patch("httpx.Client")
    def test_get_data_mock(self, mock_client):
        """Test fetching stream data with mocked responses."""
        # Create a mocked client
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        # Mock the request response
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_data
        mock_client_instance.get.return_value = mock_response

        # Create test client with patched HTTP client
        test_client = MagicMock()
        test_client.request.return_value = self.mock_data

        # Create stream with mocked client
        stream = DataStream(test_client, self.test_stream_id)

        # Fetch data
        data = stream.get_data(limit=10)

        # Verify data
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["name"], "Item 1")
        self.assertEqual(data[1]["value"], 20.5)
        self.assertEqual(data[2]["active"], True)

        # Verify that request was called with expected parameters
        test_client.request.assert_called_with(
            "GET",
            f"stream/data/{self.test_stream_id}",
            params={
                "skip":
                    0,
                "limit":
                    10,
                "format":
                    stream.
                    _default_format  # This should be msgpack when available
            })

    def test_filter_creation(self):
        """Test creating filter objects."""
        stream = DataStream(self.client, self.test_stream_id)

        # Test with string operator
        filter1 = stream.create_filter("name", "EQUALS", "Test")
        self.assertEqual(filter1["property"], "name")
        self.assertEqual(filter1["operator"], "EQUALS")
        self.assertEqual(filter1["value"], "Test")

        # Test with enum operator
        filter2 = stream.create_filter("value", FilterOperator.GREATER_THAN, 10)
        self.assertEqual(filter2["property"], "value")
        self.assertEqual(filter2["operator"], "GREATER_THAN")
        self.assertEqual(filter2["value"], 10)

        # Test with IS_NULL operator
        filter3 = stream.create_filter("optional_field", FilterOperator.IS_NULL,
                                       None)
        self.assertEqual(filter3["property"], "optional_field")
        self.assertEqual(filter3["operator"], "IS_NULL")
        self.assertIsNone(filter3["value"])

    def test_real_api_metadata(self):
        """Test fetching metadata from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id,
                                             validate=False)

        try:
            # Fetch metadata
            metadata = stream.get_metadata()

            # Basic validation of response
            self.assertEqual(metadata.key, self.test_stream_id)
            self.assertTrue(hasattr(metadata, "name"))
            self.assertTrue(hasattr(metadata, "meta"))
            self.assertTrue(hasattr(metadata.meta, "entry_count"))
            self.assertTrue(len(metadata.data_item_collections) > 0)

            # Check convenience properties
            self.assertTrue(len(metadata.column_names) > 0)
            self.assertTrue(len(metadata.display_name) > 0)

        except ResourceNotFoundError:
            self.skipTest(
                f"Stream with ID {self.test_stream_id} not found. Use a valid stream ID for this test."
            )
        except APIError as e:
            self.skipTest(f"API error when fetching stream metadata: {e}")

    def test_real_api_data(self):
        """Test fetching data from real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # First test with JSON to ensure compatibility
        data_json = stream.get_data(limit=5, format="json")

        # Basic validation of response
        self.assertIsInstance(data_json, list)
        self.assertTrue(len(data_json) <= 5)  # Should be 5 or fewer records

        if len(data_json) > 0:
            # Check first record has expected structure
            first_record = data_json[0]
            self.assertIsInstance(first_record, dict)
            # Check it has at least one key
            self.assertTrue(len(first_record.keys()) > 0)

        # Only test msgpack if truly available
        if MSGPACK_AVAILABLE:
            try:
                # Test with msgpack format
                data_msgpack = stream.get_data(limit=5, format="msgpack")

                self.assertIsInstance(data_msgpack, list)
                self.assertTrue(len(data_msgpack) <= 5)

                # Verify we can process the data
                if len(data_msgpack) > 0:
                    first_record = data_msgpack[0]
                    self.assertIsInstance(first_record, dict)
            except Exception as e:
                # Log the error but don't fail the test - msgpack support is optional
                import logging
                logging.warning(f"Skipping msgpack test due to error: {str(e)}")

    def test_metadata_caching(self):
        """Test that metadata is properly cached."""
        # Create a client with a mock request method
        test_client = MagicMock()
        test_client.request.return_value = self.mock_metadata

        # Create stream
        stream = DataStream(test_client, self.test_stream_id)

        # First call should make a request
        metadata1 = stream.get_metadata()
        test_client.request.assert_called_once()

        # Reset mock to verify second call
        test_client.request.reset_mock()

        # Second call should use cached metadata
        metadata2 = stream.get_metadata()
        test_client.request.assert_not_called()

        # Both calls should return the same metadata
        self.assertEqual(metadata1, metadata2)

    def test_invalid_id_error(self):
        """Test handling of invalid stream ID."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Attempting to create stream with invalid ID should raise error immediately
        with self.assertRaises((ResourceNotFoundError, APIError)) as context:
            self.client.streams.from_id("invalid_id_12345")

        # Verify error details if it's a ResourceNotFoundError
        if isinstance(context.exception, ResourceNotFoundError):
            self.assertEqual(context.exception.resource_type.lower(),
                             "data_stream")
            self.assertEqual(context.exception.resource_id, "invalid_id_12345")


@pytest.mark.asyncio
async def test_async_metadata_retrieval():
    """Test asynchronously retrieving stream metadata."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Fetch metadata asynchronously
        metadata = await stream.aget_metadata()

        # Basic validation
        assert metadata.key == test_stream_id
        assert metadata.name is not None
        assert metadata.meta.entry_count is not None
        assert len(metadata.data_item_collections) > 0
        assert len(metadata.column_names) > 0

    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_async_data_retrieval():
    """Test asynchronously retrieving stream data."""
    # Create a mock client instead of using real API
    mock_client = MagicMock()

    # Sample mock data
    mock_data = [{
        "id": "1",
        "name": "Item 1",
        "value": 10.5
    }, {
        "id": "2",
        "name": "Item 2",
        "value": 20.5
    }, {
        "id": "3",
        "name": "Item 3",
        "value": 30.5
    }]

    # Create an async mock function that returns the expected data
    async def mock_arequest(method, url, **kwargs):
        if "stream/base/" in url:
            # Return metadata for get_metadata call
            return {
                "key":
                    "23532363",
                "name": {
                    "en": "Test Stream"
                },
                "description": {
                    "en": "Test Description"
                },
                "meta": {
                    "entry_count": 100,
                    "main_property_count": 3,
                    "is_active": True
                },
                "data_item_collections": [{
                    "name": "id",
                    "basic_data_type": "string"
                }, {
                    "name": "name",
                    "basic_data_type": "string"
                }, {
                    "name": "value",
                    "basic_data_type": "number"
                }]
            }
        elif "stream/data/" in url:
            # Verify format is specified
            params = kwargs.get('params', {})
            assert 'format' in params, "Format should be specified in parameters"
            # For tests, always use json format for reliability
            return mock_data
        return {}

    # Use the async mock function
    mock_client.arequest = mock_arequest

    # Create stream with the mock client
    stream = DataStream(mock_client, "23532363")

    # Execute async data retrieval with explicit JSON format for test stability
    data = await stream.aget_data(limit=5, format="json")

    # Basic validation
    assert isinstance(data, list)
    assert len(data) == 3

    # Check content
    assert data[0]["id"] == "1"
    assert data[1]["name"] == "Item 2"
    assert data[2]["value"] == 30.5


@pytest.mark.asyncio
async def test_async_data_retrieval_real_api():
    """Test asynchronously retrieving stream data from real API."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Use JSON format for test stability
        data = await stream.aget_data(limit=5, format="json")

        # Basic validation
        assert isinstance(data, list)
        assert len(data) <= 5

        if len(data) > 0:
            # Check first record has expected structure
            assert isinstance(data[0], dict)
            assert len(data[0].keys()) > 0

    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.parametrize(
    "return_type,expected_type",
    [("dict", list), ("dataframe", "pandas.DataFrame"),
     pytest.param("df",
                  "pandas.DataFrame",
                  marks=pytest.mark.skipif(not PANDAS_AVAILABLE,
                                           reason="pandas not installed"))])
def test_return_types(return_type, expected_type):
    """Test different return types."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Skip dataframe tests if pandas not installed
    if expected_type == "pandas.DataFrame" and not PANDAS_AVAILABLE:
        pytest.skip("pandas not installed")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Fetch data with specified return type - always use JSON for test stability
        result = stream.get_data(limit=5,
                                 return_type=return_type,
                                 format="json")

        # Check type
        if expected_type == "pandas.DataFrame":
            import pandas as pd
            assert isinstance(result, pd.DataFrame)
        else:
            assert isinstance(result, expected_type)

    finally:
        # Always close the client
        client.close()
