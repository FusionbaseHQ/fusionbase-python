"""Tests for DataStream pandas integration."""

import unittest
from unittest.mock import MagicMock

import pytest
from conftest import get_api_key

from fusionbase import Fusionbase
from fusionbase.data.datastream import DataStream
from fusionbase.data.datastream import FilterOperator

# Skip all tests in this module if pandas is not available
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None


@unittest.skipIf(not PANDAS_AVAILABLE, "pandas not installed")
class TestDataStreamPandas(unittest.TestCase):
    """Test cases for DataStream pandas integration."""

    def setUp(self):
        """Set up test fixtures."""
        if not PANDAS_AVAILABLE:
            self.skipTest("pandas not installed")

        self.test_stream_id = "23532363"

        # Use API key from environment or skip tests if not available
        self.api_key = get_api_key()
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Sample mock data for DataFrame conversion
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

    def test_to_pandas_mock(self):
        """Test to_pandas method with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()

        # Set up the mock to return metadata first, then data
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
                    "entry_count": 3,
                    "main_property_count": 5,
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
                }, {
                    "name": "created_at",
                    "basic_data_type": "datetime"
                }, {
                    "name": "active",
                    "basic_data_type": "boolean"
                }]
            },
            self.mock_data  # Second call returns data
        ]

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Convert to DataFrame using get_data with return_type="dataframe"
        df = stream.get_data(return_type="dataframe")

        # Verify DataFrame has expected structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (3, 5))  # 3 rows, 5 columns
        self.assertEqual(list(df.columns),
                         ["id", "name", "value", "created_at", "active"])

        # Verify data is correctly converted
        self.assertEqual(df["id"].tolist(), ["1", "2", "3"])
        self.assertEqual(df["name"].tolist(), ["Item 1", "Item 2", "Item 3"])
        self.assertEqual(df["value"].tolist(), [10.5, 20.5, 30.5])

        # Verify client was called correctly - expect exactly 2 calls
        self.assertEqual(mock_client.request.call_count, 2)

        # Check that the first call was for metadata
        first_call = mock_client.request.call_args_list[0]
        self.assertEqual(first_call[0][0], "GET")
        self.assertEqual(first_call[0][1], f"stream/base/{self.test_stream_id}")

        # Check that the second call was for data
        second_call = mock_client.request.call_args_list[1]
        self.assertEqual(second_call[0][0], "GET")
        self.assertEqual(second_call[0][1],
                         f"stream/data/{self.test_stream_id}")

    def test_get_data_with_dataframe_return_type_mock(self):
        """Test get_data with DataFrame return type."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.return_value = self.mock_data

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Get data as DataFrame
        df = stream.get_data(return_type="dataframe")

        # Verify DataFrame has expected structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (3, 5))

        # Verify data is correctly converted
        self.assertEqual(df["id"].tolist(), ["1", "2", "3"])

    def test_to_pandas_with_filters_mock(self):
        """Test to_pandas with filters."""
        # Create a mock client
        mock_client = MagicMock()

        # Mock filtered response (only active=True records)
        filtered_data = [{
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
        mock_client.request.return_value = filtered_data

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Create filter for active=True
        filter_active = stream.create_filter("active", FilterOperator.EQUALS,
                                             True)

        # Convert to DataFrame with filter using get_data with return_type="dataframe"
        df = stream.get_data(filters=[filter_active], return_type="dataframe")

        # Verify DataFrame has expected structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 5))  # 2 rows (filtered), 5 columns

        # Verify filtered data is correctly converted
        self.assertEqual(df["id"].tolist(), ["1", "3"])
        self.assertTrue(all(df["active"]))

    def test_iter_chunks_pandas_mock(self):
        """Test iter_chunks_pandas with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()

        # Mock metadata and chunk responses
        mock_metadata = {"meta": {"entry_count": 500}}
        mock_chunks = [
            # First chunk
            [{
                "id": f"item{i}",
                "value": i
            } for i in range(1, 101)],
            # Second chunk
            [{
                "id": f"item{i}",
                "value": i
            } for i in range(101, 201)]
        ]

        mock_client.request.side_effect = [mock_metadata] + mock_chunks

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Process chunks as DataFrames
        dfs = []
        for df_chunk in stream.iter_chunks_pandas(chunk_size=100):
            dfs.append(df_chunk)
            if len(dfs) >= 2:
                break

        # Verify we got two DataFrame chunks
        self.assertEqual(len(dfs), 2)

        # Each chunk should be a DataFrame with the expected size
        self.assertIsInstance(dfs[0], pd.DataFrame)
        self.assertEqual(dfs[0].shape[0], 100)
        self.assertEqual(dfs[1].shape[0], 100)

        # Verify data is correctly converted
        self.assertEqual(dfs[0]["id"].iloc[0], "item1")
        self.assertEqual(dfs[1]["id"].iloc[0], "item101")

    def test_real_api_to_pandas(self):
        """Test to_pandas with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # Get metadata to determine column count
        metadata = stream.get_metadata()
        metadata.meta.main_property_count or len(metadata.column_names)

        # Skip test if stream is empty
        if metadata.meta.entry_count == 0:
            self.skipTest("Stream is empty, can't test pandas conversion")

        # Convert to DataFrame with limit - explicitly use JSON format for stability
        # Use get_data with return_type="dataframe" instead of to_pandas
        df = stream.get_data(limit=10, return_type="dataframe", _format="json")

        # Verify DataFrame structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(0 < df.shape[0] <= 10)  # Should have 10 or fewer rows
        self.assertTrue(df.shape[1] > 0)  # Should have at least one column

    def test_real_api_dataframe_return_type(self):
        """Test get_data with DataFrame return type with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # Get data as DataFrame with limit - explicitly use JSON format for stability
        df = stream.get_data(limit=10, return_type="dataframe", _format="json")

        # Verify DataFrame structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(0 < df.shape[0] <= 10)  # Should have 10 or fewer rows
        self.assertTrue(df.shape[1] > 0)  # Should have at least one column

        # Test with "df" alias for return_type - explicitly use JSON format
        df2 = stream.get_data(limit=10, return_type="df", _format="json")
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(df.shape[0], df2.shape[0])

    def test_real_api_iter_chunks_pandas(self):
        """Test iter_chunks_pandas with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # Get metadata
        metadata = stream.get_metadata()

        # Skip test if stream is too small
        if metadata.meta.entry_count < 10:
            self.skipTest(
                f"Stream has only {metadata.meta.entry_count} records, need at least 10"
            )

        # Process chunks as DataFrames - explicitly use JSON format for stability
        dfs = []
        for df_chunk in stream.iter_chunks_pandas(chunk_size=5, _format="json"):
            dfs.append(df_chunk)
            if len(dfs) >= 2:
                break

        # Verify we got DataFrame chunks
        self.assertTrue(len(dfs) > 0)
        self.assertIsInstance(dfs[0], pd.DataFrame)
        self.assertEqual(dfs[0].shape[0],
                         5)  # First chunk should have exactly 5 rows


# Async tests for pandas integration
@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
@pytest.mark.asyncio
async def test_async_to_pandas():
    """Test asynchronous pandas conversion."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Convert to DataFrame asynchronously using aget_data with return_type="dataframe"
        df = await stream.aget_data(limit=10,
                                    _format="json",
                                    return_type="dataframe")

        # Verify DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert 0 < df.shape[0] <= 10  # Should have 10 or fewer rows
        assert df.shape[1] > 0  # Should have at least one column

    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
@pytest.mark.asyncio
async def test_async_iter_chunks_pandas():
    """Test asynchronous DataFrame chunk iteration."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Get metadata
        metadata = await stream.aget_metadata()

        # Skip test if stream is too small
        if metadata.meta.entry_count < 10:
            pytest.skip(
                f"Stream has only {metadata.meta.entry_count} records, need at least 10"
            )

        # Process chunks as DataFrames asynchronously - explicitly use JSON format for stability
        dfs = []
        async for df_chunk in stream.aiter_chunks_pandas(chunk_size=5,
                                                         _format="json"):
            dfs.append(df_chunk)
            if len(dfs) >= 2:
                break

        # Verify we got DataFrame chunks
        assert len(dfs) > 0
        assert isinstance(dfs[0], pd.DataFrame)
        assert dfs[0].shape[0] == 5  # First chunk should have exactly 5 rows

    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
@pytest.mark.parametrize("limit,expected_rows", [(5, 5), (100, 100)])
def test_pandas_row_count(limit, expected_rows):
    """Test that pandas conversion returns correct row counts."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        # Get metadata
        metadata = stream.get_metadata()

        # Skip test if stream is too small
        if metadata.meta.entry_count < limit:
            pytest.skip(
                f"Stream has only {metadata.meta.entry_count} records, need at least {limit}"
            )

        # Convert to DataFrame using get_data instead of to_pandas
        df = stream.get_data(limit=limit,
                             _format="json",
                             return_type="dataframe")

        # Verify row count
        assert df.shape[0] == expected_rows

    finally:
        # Close the client
        client.close()
