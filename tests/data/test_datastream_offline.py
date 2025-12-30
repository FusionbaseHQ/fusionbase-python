"""Tests for DataStream offline mode and file operations."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from conftest import get_api_key

from fusionbase import Fusionbase
from fusionbase.data.datastream import DataStream


class TestDataStreamOffline(unittest.TestCase):
    """Test cases for DataStream offline mode and file operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_stream_id = "23532363"

        # Use API key from environment or skip tests if not available
        self.api_key = get_api_key()
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

        # Sample mock data for file operations
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

        # Sample mock metadata for file operations - add required data_item_collections field
        self.mock_metadata = {
            "id":
                f"data_streams/{self.test_stream_id}",
            "key":
                self.test_stream_id,
            "name": {
                "en": "Test Stream",
                "de": "Teststrom"
            },
            "description": {
                "en": "Test stream for offline tests",
                "de": "Teststrom für Offline-Tests"
            },
            "meta": {
                "entry_count": 3,
                "main_property_count": 5,
                "is_active": True
            },
            "data_version":
                "v1.0",
            # Add the required data_item_collections field
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
        }

        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

        # Clean up temporary directory
        if hasattr(self, "temp_dir"):
            self.temp_dir.cleanup()

    def test_export_to_json(self):
        """Test exporting stream data to JSON file."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.side_effect = [
            self.mock_metadata,  # For get_metadata
            self.mock_data  # For get_data
        ]

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Create a JSON file path in temp directory
        json_path = Path(self.temp_dir.name) / "test_data.json"

        # Export to JSON
        stream.export_to_file(json_path, include_metadata=True)

        # Verify the file was created
        self.assertTrue(json_path.exists())

        # Check file content
        with open(json_path, "r") as f:
            file_content = json.load(f)

        # Check structure - should have both data and metadata
        self.assertIn("data", file_content)
        self.assertIn("metadata", file_content)
        self.assertEqual(len(file_content["data"]), 3)

    def test_export_to_jsonl(self):
        """Test exporting stream data to JSONL file."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.side_effect = [
            self.mock_metadata,  # For get_metadata
            self.mock_data  # For get_data
        ]

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Create a JSONL file path in temp directory
        jsonl_path = Path(self.temp_dir.name) / "test_data.jsonl"

        # Export to JSONL
        stream.export_to_file(jsonl_path,
                              file_format="jsonl",
                              include_metadata=True)

        # Verify the file was created
        self.assertTrue(jsonl_path.exists())

        # Check file content - JSONL has one JSON object per line
        with open(jsonl_path, "r") as f:
            lines = f.readlines()

        # Should have 3 data lines + 1 metadata line
        self.assertEqual(len(lines), 4)

        # First line should be metadata
        metadata_line = json.loads(lines[0])
        self.assertIn("_type", metadata_line)
        self.assertEqual(metadata_line["_type"], "metadata")

        # Check a data line
        data_line = json.loads(lines[1])
        self.assertIn("_type", data_line)
        self.assertEqual(data_line["_type"], "data")

    @unittest.skipIf(not hasattr(DataStream, "load_from_file"),
                     "load_from_file not implemented")
    def test_load_from_json(self):
        """Test loading data from a JSON file."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.request.side_effect = [
            self.mock_metadata,  # For get_metadata
            self.mock_data  # For get_data
        ]

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Create a JSON file path in temp directory
        json_path = Path(self.temp_dir.name) / "test_data.json"

        # First export to JSON
        stream.export_to_file(json_path, include_metadata=True)

        # Now load the data from the file
        data, metadata = stream.load_from_file(json_path)

        # Verify the data was loaded correctly
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["id"], "1")
        self.assertEqual(data[1]["name"], "Item 2")
        self.assertEqual(data[2]["value"], 30.5)

        # Verify metadata was loaded
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata["key"], self.test_stream_id)

    def test_offline_mode(self):
        """Test using a DataStream in offline mode."""
        # First create a real stream directly (without using the manager)
        stream = DataStream(client=self.client,
                            stream_identifier=self.test_stream_id,
                            live=False,
                            cache_dir=self.temp_dir.name)

        # Check that cache file doesn't exist yet
        cache_path = Path(self.temp_dir.name) / f"{self.test_stream_id}.json"
        if cache_path.exists():
            # Delete any existing cache file
            cache_path.unlink()

        self.assertFalse(cache_path.exists())

        # Get some data, which should create the cache file - explicitly use JSON format for test stability
        data = stream.get_data(limit=10, _format="json")

        # Verify we got data
        self.assertIsInstance(data, list)
        self.assertTrue(len(data) > 0)

        # Check that cache file was created
        self.assertTrue(cache_path.exists())

        # Now create a new offline stream pointing to the same cache
        offline_stream = DataStream(client=self.client,
                                    stream_identifier=self.test_stream_id,
                                    live=False,
                                    cache_dir=self.temp_dir.name)

        # Get data - should use cache - explicitly use JSON format for test stability
        with patch.object(self.client, 'request') as mock_request:
            # Mock should not be called since we use cache
            offline_data = offline_stream.get_data(limit=10, _format="json")
            mock_request.assert_not_called()

        # Verify we got the same data
        self.assertEqual(len(data), len(offline_data))

        # Force live mode to bypass cache - explicitly use JSON format for test stability
        with patch.object(self.client, 'request') as mock_request:
            # Mock should be called since we force live mode
            offline_stream.get_data(limit=10, force_live=True, _format="json")
            mock_request.assert_called()

    @unittest.skipIf(not hasattr(DataStream, "from_file"),
                     "from_file not implemented")
    def test_create_from_file(self):
        """Test creating a DataStream directly from a file."""
        # Create a mock client
        mock_client = MagicMock()

        # Create a complete metadata mock that includes all required fields
        complete_metadata = {
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
            }],
            "data_version": "v1.0"
        }

        # Set up the mock to return the complete metadata
        mock_client.request.side_effect = [
            complete_metadata,  # First call for get_metadata
            self.mock_data  # Second call for get_data
        ]

        # Create stream with mocked client
        stream = DataStream(mock_client, self.test_stream_id)

        # Create a JSON file path in temp directory
        json_path = Path(self.temp_dir.name) / "test_data.json"

        # Export to JSON
        stream.export_to_file(json_path, include_metadata=True)

        # Verify the file was created
        self.assertTrue(json_path.exists())

        # Create a new DataStream directly from the file
        file_stream = DataStream.from_file(json_path)

        # Verify the stream was created correctly
        self.assertIsNotNone(file_stream)

        # Check that we can get data from the file-based stream
        file_data = file_stream.get_data()

        # Verify the data
        self.assertEqual(len(file_data), 3)
        self.assertEqual(file_data[0]["id"], "1")

    def test_real_api_file_export(self):
        """Test file export with real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create stream using real API
        stream = self.client.streams.from_id(self.test_stream_id)

        # Get metadata to ensure the stream exists
        metadata = stream.get_metadata()

        # Skip test if stream is empty
        if metadata.meta.entry_count == 0:
            self.skipTest("Stream is empty, can't test file export")

        # Create a JSON file path in temp directory
        json_path = Path(self.temp_dir.name) / "real_data.json"

        # Export to JSON with a small limit - explicitly use JSON format to avoid binary format issues
        stream.export_to_file(json_path,
                              limit=5,
                              include_metadata=True,
                              file_format="json")

        # Verify the file was created
        self.assertTrue(json_path.exists())

        # Check file content
        with open(json_path, "r") as f:
            file_content = json.load(f)

        # Check structure - should have both data and metadata
        self.assertIn("data", file_content)
        self.assertIn("metadata", file_content)

        # Should have at most 5 records
        self.assertTrue(0 < len(file_content["data"]) <= 5)

        # Metadata should match the stream
        self.assertEqual(file_content["metadata"]["key"], self.test_stream_id)


@pytest.mark.asyncio
async def test_async_file_export():
    """Test asynchronous file export."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    test_stream_id = "23532363"

    # Create client
    client = Fusionbase(api_key=api_key)

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Create stream
            stream = client.streams.from_id(test_stream_id)

            # Create a JSON file path in temp directory
            json_path = Path(temp_dir) / "async_data.json"

            # Export to JSON asynchronously - explicitly use JSON format for stability
            await stream.aexport_to_file(json_path,
                                         limit=5,
                                         include_metadata=True,
                                         file_format="json")

            # Verify the file was created
            assert json_path.exists()

            # Check file content
            with open(json_path, "r") as f:
                file_content = json.load(f)

            # Check structure - should have both data and metadata
            assert "data" in file_content
            assert "metadata" in file_content

            # Should have at most 5 records
            assert 0 < len(file_content["data"]) <= 5

            # Metadata should match the stream
            assert file_content["metadata"]["key"] == test_stream_id

        finally:
            # Always close the client
            await client.aclose()
            client.close()
