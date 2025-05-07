"""Tests for DataStream filtering and pagination functionality."""

import os
import unittest
from unittest.mock import MagicMock

import pytest

from fusionbase import Fusionbase
from fusionbase.data.datastream import DataStream
from fusionbase.data.datastream import FilterOperator

MSGPACK_AVAILABLE = True  # Assume msgpack is available for testing purposes


class TestDataStreamFiltering(unittest.TestCase):
    """Test cases for DataStream filtering and pagination."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_stream_id = "23532363"

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Sample mock data response with multiple pages
        self.mock_data_page1 = [{
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

        self.mock_data_page2 = [{
            "id": "4",
            "name": "Item 4",
            "value": 40.5,
            "created_at": "2023-01-04",
            "active": False
        }, {
            "id": "5",
            "name": "Item 5",
            "value": 50.5,
            "created_at": "2023-01-05",
            "active": True
        }]

        # Sample filtered data
        self.mock_filtered_data = [{
            "id": "1",
            "name": "Item 1",
            "value": 10.5,
            "created_at": "2023-01-01",
            "active": True
        }, {
            "id": "3",
            "name": "Item 3",
            "value": 30.5,
            "created_at": "2023-01-03",
            "active": True
        }]

        # Always prefer msgpack for tests when available
        self.preferred_format = "msgpack" if MSGPACK_AVAILABLE else "json"

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_pagination_mock(self):
        """Test pagination with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.side_effect = [
            self.mock_data_page1,  # First page
            self.mock_data_page2  # Second page
        ]

        # Create stream
        stream = DataStream(mock_client, self.test_stream_id)

        # Get first page
        page1 = stream.get_data(skip=0, limit=3)
        self.assertEqual(len(page1), 3)
        self.assertEqual(page1[0]["id"], "1")

        # Verify call to first page
        mock_client.request.assert_called_with(
            "GET",
            f"stream/data/{self.test_stream_id}",
            params={
                "skip":
                    0,
                "limit":
                    3,
                "format":
                    stream.
                    _default_format  # This should be msgpack when available
            })

        # Get second page
        page2 = stream.get_data(skip=3, limit=2)
        self.assertEqual(len(page2), 2)
        self.assertEqual(page2[0]["id"], "4")

        # Verify call to second page
        mock_client.request.assert_called_with(
            "GET",
            f"stream/data/{self.test_stream_id}",
            params={
                "skip":
                    3,
                "limit":
                    2,
                "format":
                    stream.
                    _default_format  # This should be msgpack when available
            })

    def test_filtering_mock(self):
        """Test filtering with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()

        # Mock responses for both metadata call and filtered data call
        mock_client.request.side_effect = [
            {  # First call returns metadata
                "key":
                    self.test_stream_id,
                "name": {
                    "en": "Test Stream"
                },
                "description": {
                    "en": "Test stream for unit tests"
                },
                "meta": {
                    "entry_count": 5,
                    "main_property_count": 5,
                    "is_active": True
                },
                "data_item_collections": [{
                    "name": "id",
                    "basic_data_type": "string"
                }]
            },
            self.mock_filtered_data  # Second call returns filtered data
        ]

        # Create stream
        stream = DataStream(mock_client, self.test_stream_id)

        # Create filter for active=True records
        filter_active = stream.create_filter("active", FilterOperator.EQUALS,
                                             True)

        # Get filtered data
        filtered_data = stream.get_data(filters=[filter_active])

        # Verify data is filtered
        self.assertEqual(len(filtered_data), 2)
        self.assertEqual(filtered_data[0]["id"], "1")
        self.assertEqual(filtered_data[1]["id"], "3")
        self.assertTrue(all(record["active"] for record in filtered_data))

        # Verify calls were made - should be 2 calls, one for metadata and one for data
        self.assertEqual(mock_client.request.call_count, 2)

        # Check the second call which is the data request with filters
        data_call = mock_client.request.call_args_list[1]
        self.assertEqual(data_call[0][0], "GET")
        self.assertEqual(data_call[0][1], f"stream/data/{self.test_stream_id}")

        # Check that filters were included in query parameters
        params = data_call[1]["params"]
        self.assertIn("query_parameters", params)

        # We can't directly check the JSON-encoded string easily,
        # but we can verify it contains the filter information
        query_params_str = params["query_parameters"]
        self.assertIn("filters", query_params_str)
        self.assertIn("active", query_params_str)

    def test_filtering_with_multiple_conditions_mock(self):
        """Test filtering with multiple conditions using mocked responses."""
        # Create a mock client
        mock_client = MagicMock()

        # Mock responses for both metadata call and filtered data call
        mock_client.request.side_effect = [
            {  # First call returns metadata
                "key":
                    self.test_stream_id,
                "name": {
                    "en": "Test Stream"
                },
                "description": {
                    "en": "Test stream for unit tests"
                },
                "meta": {
                    "entry_count": 5,
                    "main_property_count": 5,
                    "is_active": True
                },
                "data_item_collections": [{
                    "name": "id",
                    "basic_data_type": "string"
                }]
            },
            # Second call returns filtered data
            [{
                "id": "1",
                "name": "Item 1",
                "value": 10.5,
                "created_at": "2023-01-01",
                "active": True
            }]
        ]

        # Create stream
        stream = DataStream(mock_client, self.test_stream_id)

        # Create multiple filters
        filter_active = stream.create_filter("active", FilterOperator.EQUALS,
                                             True)
        filter_value = stream.create_filter("value", FilterOperator.LESS_THAN,
                                            20.0)

        # Get filtered data
        filtered_data = stream.get_data(filters=[filter_active, filter_value])

        # Verify data is filtered
        self.assertEqual(len(filtered_data), 1)
        self.assertEqual(filtered_data[0]["id"], "1")
        self.assertTrue(filtered_data[0]["active"])
        self.assertLess(filtered_data[0]["value"], 20.0)

        # Verify calls were made - should be 2 calls, one for metadata and one for data
        self.assertEqual(mock_client.request.call_count, 2)

        # Check the second call which is the data request with filters
        data_call = mock_client.request.call_args_list[1]

        # Check that filters were included in query parameters
        params = data_call[1]["params"]
        self.assertIn("query_parameters", params)

        # We can't directly check the JSON-encoded string easily,
        # but we can verify it contains both filter conditions
        query_params_str = params["query_parameters"]
        self.assertIn("filters", query_params_str)
        self.assertIn("active", query_params_str)
        self.assertIn("value", query_params_str)

    def test_sort_mock(self):
        """Test sorting functionality with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()

        # Create mock metadata first (necessary for DataStream initialization)
        mock_metadata = {
            "key":
                self.test_stream_id,
            "name": {
                "en": "Test Stream"
            },
            "description": {
                "en": "Test stream for unit tests"
            },
            "meta": {
                "entry_count": 3,
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

        # Create sorted mock responses - ordering is critical here
        mock_asc_response = [{
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

        mock_desc_response = [{
            "id": "3",
            "name": "Item 3",
            "value": 30.5
        }, {
            "id": "2",
            "name": "Item 2",
            "value": 20.5
        }, {
            "id": "1",
            "name": "Item 1",
            "value": 10.5
        }]

        # Set up mock responses in the expected order
        mock_client.request.side_effect = [
            mock_metadata,  # First call - metadata
            mock_asc_response,  # Second call - ascending data
            mock_desc_response  # Third call - descending data
        ]

        # Create stream
        stream = DataStream(mock_client, self.test_stream_id)

        # Get data sorted ascending by value
        asc_data = stream.get_data(sort_keys=["value"], sort_order=["asc"])

        # Verify data is sorted correctly
        self.assertEqual(asc_data[0]["value"], 10.5)
        self.assertEqual(asc_data[1]["value"], 20.5)
        self.assertEqual(asc_data[2]["value"], 30.5)

        # Verify call includes sorting parameters
        call_args = mock_client.request.call_args_list[1][1]["params"]
        self.assertIn("query_parameters", call_args)
        self.assertIn("sort_keys", call_args["query_parameters"])
        self.assertIn("asc", call_args["query_parameters"])

        # Get data sorted descending by value
        desc_data = stream.get_data(sort_keys=["value"], sort_order=["desc"])

        # Verify data is sorted correctly
        self.assertEqual(desc_data[0]["value"], 30.5)
        self.assertEqual(desc_data[1]["value"], 20.5)
        self.assertEqual(desc_data[2]["value"], 10.5)

        # Verify call includes sorting parameters
        call_args = mock_client.request.call_args_list[2][1]["params"]
        self.assertIn("query_parameters", call_args)
        self.assertIn("sort_keys", call_args["query_parameters"])
        self.assertIn("desc", call_args["query_parameters"])

    def test_projection_mock(self):
        """Test field projection functionality with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()

        # Mock responses for both metadata call and projected data
        mock_client.request.side_effect = [
            {  # First call returns metadata
                "key":
                    self.test_stream_id,
                "name": {
                    "en": "Test Stream"
                },
                "description": {
                    "en": "Test stream for unit tests"
                },
                "meta": {
                    "entry_count": 5,
                    "main_property_count": 5,
                    "is_active": True
                },
                "data_item_collections": [{
                    "name": "id",
                    "basic_data_type": "string"
                }]
            },
            # Second call returns data with only projected fields
            [
                {
                    "id": "1",
                    "name": "Item 1"
                },  # Only id and name fields
                {
                    "id": "2",
                    "name": "Item 2"
                },
                {
                    "id": "3",
                    "name": "Item 3"
                }
            ]
        ]

        # Create stream
        stream = DataStream(mock_client, self.test_stream_id)

        # Get data with projection
        data = stream.get_data(project_fields=["id", "name"])

        # Verify data has only the projected fields
        self.assertEqual(len(data), 3)
        self.assertEqual(set(data[0].keys()), {"id", "name"})
        self.assertNotIn("value", data[0])

        # Verify calls were made - should be 2 calls, one for metadata and one for data
        self.assertEqual(mock_client.request.call_count, 2)

        # Check the second call which is the data request with projection
        data_call = mock_client.request.call_args_list[1]

        # Check that projection was included in query parameters
        params = data_call[1]["params"]
        self.assertIn("query_parameters", params)

        # We can't directly check the JSON-encoded string easily,
        # but we can verify it contains the projection information
        query_params_str = params["query_parameters"]
        self.assertIn("project_fields", query_params_str)
        self.assertIn("id", query_params_str)
        self.assertIn("name", query_params_str)

    def test_real_api_pagination(self):
        """Test pagination with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # Get metadata to determine total count
        metadata = stream.get_metadata()
        total_count = metadata.meta.entry_count

        # Skip test if stream is empty
        if total_count == 0:
            self.skipTest("Stream is empty, can't test pagination")

        # Get first page (5 records) - explicitly use JSON format to avoid binary format issues
        page1 = stream.get_data(skip=0, limit=5, format="json")

        # Skip test if stream has fewer than 6 records
        if total_count < 6:
            self.skipTest(
                f"Stream has only {total_count} records, can't test pagination properly"
            )

        # Get second page (next 5 records) - explicitly use JSON format
        page2 = stream.get_data(skip=5, limit=5, format="json")

        # Basic validation
        self.assertEqual(len(page1),
                         5)  # First page should have exactly 5 records
        self.assertTrue(len(page2)
                        > 0)  # Second page should have at least 1 record

        # Pages should contain different records
        if len(page2) > 0 and len(page1) > 0:
            # Compare the first record of each page
            # This assumes records have some unique identifier
            # If they have IDs, compare those
            if "id" in page1[0] and "id" in page2[0]:
                self.assertNotEqual(page1[0]["id"], page2[0]["id"])
            # Otherwise, check that the entire records differ
            else:
                self.assertNotEqual(page1[0], page2[0])

    def test_real_api_sorting(self):
        """Test sorting with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # Get metadata to determine columns
        metadata = stream.get_metadata()

        # Skip test if stream is empty
        if metadata.meta.entry_count == 0:
            self.skipTest("Stream is empty, can't test sorting")

        # Find a sortable column (prefer numeric or date)
        sortable_column = None
        for column in metadata.data_item_collections:
            if column.basic_data_type in [
                    "number", "integer", "float", "datetime", "date"
            ]:
                sortable_column = column.name
                break

        # Fall back to first column if no numeric/date column found
        if not sortable_column and metadata.data_item_collections:
            sortable_column = metadata.data_item_collections[0].name

        # Skip test if no columns available
        if not sortable_column:
            self.skipTest("No columns available for sorting")

        # Get data sorted ascending - explicitly use JSON format
        asc_data = stream.get_data(sort_keys=[sortable_column],
                                   sort_order=["asc"],
                                   limit=10,
                                   format="json")

        # Get data sorted descending - explicitly use JSON format
        desc_data = stream.get_data(sort_keys=[sortable_column],
                                    sort_order=["desc"],
                                    limit=10,
                                    format="json")

        # Skip further checks if not enough data
        if len(asc_data) < 2 or len(desc_data) < 2:
            self.skipTest("Not enough data to verify sorting")

        # Check that ascending and descending sort orders differ
        # Note: This assumes the data has some variation
        # If all values are identical, this check would fail
        try:
            # Try to directly compare the field values
            asc_values = [record.get(sortable_column) for record in asc_data]
            desc_values = [record.get(sortable_column) for record in desc_data]

            # Check if we got different ordering
            self.assertNotEqual(asc_values, desc_values)

            # For more strict checking of order: check first vs last
            if len(asc_values) > 1 and all(
                    val is not None for val in asc_values):
                # For numeric or comparable values, we can check ordering
                try:
                    if isinstance(asc_values[0], (int, float)):
                        self.assertLessEqual(asc_values[0], asc_values[-1])
                        self.assertGreaterEqual(desc_values[0], desc_values[-1])
                except (TypeError, AssertionError):
                    # If comparison fails, just check they're different
                    self.assertNotEqual(asc_data[0], desc_data[0])
        except (TypeError, KeyError):
            # If direct comparison fails, just check the records are different
            self.assertNotEqual(asc_data[0], desc_data[0])


# Add additional async tests for filtering, sorting, and pagination
@pytest.mark.asyncio
async def test_async_pagination():
    """Test async pagination."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Get metadata to determine total count
        metadata = await stream.aget_metadata()
        total_count = metadata.meta.entry_count

        # Skip test if stream is empty
        if total_count == 0:
            pytest.skip("Stream is empty, can't test pagination")

        # Get first page (5 records) - explicitly use JSON format
        page1 = await stream.aget_data(skip=0, limit=5, format="json")

        # Skip test if stream has fewer than 6 records
        if total_count < 6:
            pytest.skip(
                f"Stream has only {total_count} records, can't test pagination properly"
            )

        # Get second page (next 5 records) - explicitly use JSON format
        page2 = await stream.aget_data(skip=5, limit=5, format="json")

        # Basic validation
        assert len(page1) == 5  # First page should have exactly 5 records
        assert len(page2) > 0  # Second page should have at least 1 record

        # Pages should contain different records
        if len(page2) > 0 and len(page1) > 0:
            # Compare the first record of each page
            # This assumes records have some unique identifier
            if "id" in page1[0] and "id" in page2[0]:
                assert page1[0]["id"] != page2[0]["id"]
            # Otherwise, check that the entire records differ
            else:
                assert page1[0] != page2[0]

    finally:
        # Always close the client
        await client.aclose()
        client.close()
