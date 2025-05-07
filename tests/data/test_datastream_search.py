"""Tests for DataStream search functionality."""

import os
import unittest
from unittest.mock import MagicMock

import pytest

from fusionbase import Fusionbase
from fusionbase.data.datastream import DataStream
from fusionbase.exceptions import APIError


class TestDataStreamSearch(unittest.TestCase):
    """Test cases for DataStream search functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_stream_id = "23532363"  # Test stream ID

        # Use API key from environment or skip tests if not available
        self.api_key = os.environ.get("FUSIONBASE_API_KEY")
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Sample mock search results
        self.mock_search_results = [{
            "id": "1",
            "name": "Item 1",
            "description": "This contains the test query"
        }, {
            "id": "3",
            "name": "Item 3",
            "description": "Another test query result"
        }]

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_search_data_mock(self):
        """Test searching stream data with mocked responses."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.return_value = self.mock_search_results

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Search for data with query
        query = "test query"
        results = stream.search_data(q=query, limit=10)

        # Verify results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], "1")
        self.assertEqual(results[1]["name"], "Item 3")

        # Verify request was made correctly
        mock_client.request.assert_called_once()
        args, kwargs = mock_client.request.call_args
        self.assertEqual(args[0], "GET")
        self.assertEqual(args[1], f"stream/data/search/{self.test_stream_id}")
        self.assertEqual(kwargs["params"]["q"], query)
        self.assertEqual(kwargs["params"]["limit"], 10)

    def test_search_data_empty_query(self):
        """Test that searching with an empty query raises an error."""
        # Create stream
        stream = DataStream(self.client, self.test_stream_id)

        # Searching with empty query should raise ValueError
        with self.assertRaises(ValueError):
            stream.search_data(q="")

    def test_real_api_search_data(self):
        """Test searching stream data with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        try:
            # Get some data first to see what we can search for
            sample_data = stream.get_data(limit=1, format="json")

            # Skip test if no data available
            if not sample_data:
                self.skipTest("No data available to search")

            # Find something to search for
            search_term = None
            for item in sample_data:
                for key, value in item.items():
                    if isinstance(value, str) and len(value) > 3:
                        search_term = value[:
                                            3]  # Take first 3 chars of a string value
                        break
                if search_term:
                    break

            # Skip test if no suitable search term found
            if not search_term:
                self.skipTest("No suitable search term found in data")

            # Search for data using that term - explicitly use JSON format for test stability
            results = stream.search_data(q=search_term, format="json")

            # Basic validation - just check that it returns a list
            # (could be empty if nothing matches)
            self.assertIsInstance(results, list)

        except APIError as e:
            # Skip if the endpoint isn't available yet
            self.skipTest(f"API error when testing search: {e}")

    def test_search_data_with_dataframe(self):
        """Test searching with DataFrame return type."""
        # Skip if pandas not available
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.return_value = self.mock_search_results

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Search with DataFrame return type
        results_df = stream.search_data(q="test", return_type="dataframe")

        # Verify it's a DataFrame with expected structure
        self.assertIsInstance(results_df, pd.DataFrame)
        self.assertEqual(results_df.shape[0], 2)  # 2 rows
        self.assertIn("id", results_df.columns)
        self.assertIn("description", results_df.columns)


# Async tests
@pytest.mark.asyncio
async def test_async_search_data_mock():
    """Test async searching for data within a stream with mocked response."""
    # Mock search results
    mock_search_results = [{
        "id": "1",
        "name": "Item 1",
        "description": "This contains the test query"
    }, {
        "id": "3",
        "name": "Item 3",
        "description": "Another test query result"
    }]

    # Create a mock client
    mock_client = MagicMock()

    # Mock the async request method
    async def mock_arequest(method, url, **kwargs):
        # Check that the request is properly formatted
        assert method == "GET"
        assert "stream/data/search/" in url
        assert "q" in kwargs["params"]
        return mock_search_results

    mock_client.arequest = mock_arequest

    # Create stream with mocked client
    stream = DataStream(mock_client, "test123")

    # Search for data with query
    query = "test query"
    results = await stream.asearch_data(q=query, limit=10)

    # Verify results
    assert len(results) == 2
    assert results[0]["id"] == "1"
    assert results[1]["name"] == "Item 3"


@pytest.mark.asyncio
async def test_async_search_data():
    """Test async searching for data within a stream."""
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    try:
        # Create stream
        stream = client.streams.from_id(test_stream_id)

        try:
            # Get some data first to see what we can search for
            sample_data = await stream.aget_data(limit=1, format="json")

            # Skip test if no data available
            if not sample_data:
                pytest.skip("No data available to search")

            # Find something to search for
            search_term = None
            for item in sample_data:
                for key, value in item.items():
                    if isinstance(value, str) and len(value) > 3:
                        search_term = value[:
                                            3]  # Take first 3 chars of a string value
                        break
                if search_term:
                    break

            # Skip test if no suitable search term found
            if not search_term:
                pytest.skip("No suitable search term found in data")

            # Search for data using that term - explicitly use JSON format for test stability
            results = await stream.asearch_data(q=search_term, format="json")

            # Basic validation
            assert isinstance(results, list)

        except APIError as e:
            # Skip if the endpoint isn't available yet
            pytest.skip(f"API error when testing search: {e}")

    finally:
        # Always close the client
        await client.aclose()
        client.close()


@pytest.mark.asyncio
async def test_async_search_data_with_dataframe():
    """Test async searching with DataFrame return type."""
    # Skip if pandas not available
    try:
        import pandas as pd
    except ImportError:
        pytest.skip("pandas not installed")

    # Create a mock client
    mock_client = MagicMock()

    # Mock the async request method
    async def mock_arequest(method, url, **kwargs):
        return [{
            "id": "1",
            "name": "Item 1",
            "value": 10.5
        }, {
            "id": "2",
            "name": "Item 2",
            "value": 20.5
        }]

    mock_client.arequest = mock_arequest

    # Create stream with mocked client
    stream = DataStream(mock_client, "test123")

    # Search with DataFrame return type
    results_df = await stream.asearch_data(q="test", return_type="dataframe")

    # Verify it's a DataFrame with expected structure
    assert isinstance(results_df, pd.DataFrame)
    assert results_df.shape[0] == 2  # 2 rows
    assert "id" in results_df.columns
    assert "value" in results_df.columns
