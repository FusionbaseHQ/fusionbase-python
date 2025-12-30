"""Tests for the Fusionbase client."""

import os
import unittest
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase import Fusionbase
from fusionbase.core.config import FusionbaseConfig
from fusionbase.core.config import RetryConfig
from fusionbase.managers.dataservice_manager import DataServiceManager
from fusionbase.managers.datastream_manager import DataStreamManager


class TestFusionbaseClient(unittest.TestCase):
    """Test cases for Fusionbase client."""

    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"

    @patch("httpx.Client")
    def test_init_defaults(self, mock_client):
        """Test initialization with default parameters."""
        client = Fusionbase(api_key=self.api_key)
        self.assertIsNotNone(client)
        self.assertEqual(client.api_key, self.api_key)
        self.assertEqual(client.base_url, "https://api.fusionbase.com/api/v2/")
        self.assertEqual(client.config.timeout, 60.0)

    def test_init_no_api_key(self):
        """Test initialization without API key."""
        # Cache existing environment variables
        api_key_cache = os.getenv("FUSIONBASE_API_KEY")
        api_key_com_cache = os.getenv("FUSIONBASE_API_KEY_COM")

        # Remove from environment (handle case where they may not exist)
        if "FUSIONBASE_API_KEY" in os.environ:
            del os.environ["FUSIONBASE_API_KEY"]
        if "FUSIONBASE_API_KEY_COM" in os.environ:
            del os.environ["FUSIONBASE_API_KEY_COM"]

        try:
            with self.assertRaises(ValueError):
                Fusionbase(api_key=None)
        finally:
            # Restore environment variables
            if api_key_cache is not None:
                os.environ["FUSIONBASE_API_KEY"] = api_key_cache
            if api_key_com_cache is not None:
                os.environ["FUSIONBASE_API_KEY_COM"] = api_key_com_cache

    @patch("httpx.Client")
    def test_init_with_config(self, mock_client):
        """Test initialization with custom config."""
        # Updated to use the new configuration structure
        config = FusionbaseConfig(timeout=120.0,
                                  retry=RetryConfig(max_attempts=5))
        client = Fusionbase(api_key=self.api_key, config=config)
        self.assertEqual(client.config.timeout, 120.0)
        self.assertEqual(client.config.retry.max_attempts,
                         5)  # Changed to access retry.max_attempts

    @patch("httpx.Client")
    def test_context_manager(self, mock_client):
        """Test using client as context manager."""
        mock_instance = mock_client.return_value
        with Fusionbase(api_key=self.api_key) as client:
            self.assertIsNotNone(client)
        mock_instance.close.assert_called_once()


class TestFusionbaseDatastreams(unittest.TestCase):
    """Test cases for Fusionbase datastreams property."""

    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"

    @patch("httpx.Client")
    def test_datastreams_property_creates_manager(self, mock_client):
        """Test that datastreams property creates DataStreamManager."""
        client = Fusionbase(api_key=self.api_key)

        datastreams = client.datastreams

        self.assertIsInstance(datastreams, DataStreamManager)

    @patch("httpx.Client")
    def test_datastreams_property_caches_manager(self, mock_client):
        """Test that datastreams property returns cached instance."""
        client = Fusionbase(api_key=self.api_key)

        datastreams1 = client.datastreams
        datastreams2 = client.datastreams

        self.assertIs(datastreams1, datastreams2)

    @patch("httpx.Client")
    def test_datastreams_property_lazy_initialization(self, mock_client):
        """Test that datastreams is lazily initialized."""
        client = Fusionbase(api_key=self.api_key)

        # Should be None before first access
        self.assertIsNone(client._datastreams)

        # Access the property
        _ = client.datastreams

        # Should now be initialized
        self.assertIsNotNone(client._datastreams)


class TestFusionbaseDataservices(unittest.TestCase):
    """Test cases for Fusionbase dataservices property."""

    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"

    @patch("httpx.Client")
    def test_dataservices_property_creates_manager(self, mock_client):
        """Test that dataservices property creates DataServiceManager."""
        client = Fusionbase(api_key=self.api_key)

        dataservices = client.dataservices

        self.assertIsInstance(dataservices, DataServiceManager)

    @patch("httpx.Client")
    def test_dataservices_property_caches_manager(self, mock_client):
        """Test that dataservices property returns cached instance."""
        client = Fusionbase(api_key=self.api_key)

        dataservices1 = client.dataservices
        dataservices2 = client.dataservices

        self.assertIs(dataservices1, dataservices2)

    @patch("httpx.Client")
    def test_dataservices_property_lazy_initialization(self, mock_client):
        """Test that dataservices is lazily initialized."""
        client = Fusionbase(api_key=self.api_key)

        # Should be None before first access
        self.assertIsNone(client._dataservices)

        # Access the property
        _ = client.dataservices

        # Should now be initialized
        self.assertIsNotNone(client._dataservices)


class TestFusionbaseGetDatastream(unittest.TestCase):
    """Test cases for get_datastream method."""

    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"

    @patch("httpx.Client")
    def test_get_datastream_calls_manager(self, mock_client):
        """Test get_datastream calls datastreams.from_id."""
        client = Fusionbase(api_key=self.api_key)

        with patch.object(DataStreamManager, 'from_id') as mock_from_id:
            mock_stream = MagicMock()
            mock_from_id.return_value = mock_stream

            result = client.get_datastream("test_stream_id")

            mock_from_id.assert_called_once_with("test_stream_id", validate=True)
            self.assertEqual(result, mock_stream)

    @patch("httpx.Client")
    def test_get_datastream_with_validate_false(self, mock_client):
        """Test get_datastream passes validate parameter."""
        client = Fusionbase(api_key=self.api_key)

        with patch.object(DataStreamManager, 'from_id') as mock_from_id:
            mock_stream = MagicMock()
            mock_from_id.return_value = mock_stream

            client.get_datastream("test_stream_id", validate=False)

            mock_from_id.assert_called_once_with("test_stream_id", validate=False)


class TestFusionbaseGetDataservice(unittest.TestCase):
    """Test cases for get_dataservice method."""

    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"

    @patch("httpx.Client")
    def test_get_dataservice_calls_manager(self, mock_client):
        """Test get_dataservice calls dataservices.from_id."""
        client = Fusionbase(api_key=self.api_key)

        with patch.object(DataServiceManager, 'from_id') as mock_from_id:
            mock_service = MagicMock()
            mock_from_id.return_value = mock_service

            result = client.get_dataservice("test_service_id")

            mock_from_id.assert_called_once_with("test_service_id", validate=True)
            self.assertEqual(result, mock_service)

    @patch("httpx.Client")
    def test_get_dataservice_with_validate_false(self, mock_client):
        """Test get_dataservice passes validate parameter."""
        client = Fusionbase(api_key=self.api_key)

        with patch.object(DataServiceManager, 'from_id') as mock_from_id:
            mock_service = MagicMock()
            mock_from_id.return_value = mock_service

            client.get_dataservice("test_service_id", validate=False)

            mock_from_id.assert_called_once_with("test_service_id", validate=False)


@pytest.mark.asyncio
async def test_aget_datastream():
    """Test async get_datastream method."""
    with patch("httpx.Client"):
        client = Fusionbase(api_key="test_api_key")

        with patch.object(DataStreamManager, 'afrom_id', new_callable=AsyncMock) as mock_afrom_id:
            mock_stream = MagicMock()
            mock_afrom_id.return_value = mock_stream

            result = await client.aget_datastream("async_stream_id")

            mock_afrom_id.assert_called_once_with("async_stream_id", validate=True)
            assert result == mock_stream


@pytest.mark.asyncio
async def test_aget_datastream_with_validate_false():
    """Test async get_datastream with validate=False."""
    with patch("httpx.Client"):
        client = Fusionbase(api_key="test_api_key")

        with patch.object(DataStreamManager, 'afrom_id', new_callable=AsyncMock) as mock_afrom_id:
            mock_stream = MagicMock()
            mock_afrom_id.return_value = mock_stream

            await client.aget_datastream("async_stream_id", validate=False)

            mock_afrom_id.assert_called_once_with("async_stream_id", validate=False)


@pytest.mark.asyncio
async def test_aget_dataservice():
    """Test async get_dataservice method."""
    with patch("httpx.Client"):
        client = Fusionbase(api_key="test_api_key")

        with patch.object(DataServiceManager, 'afrom_id', new_callable=AsyncMock) as mock_afrom_id:
            mock_service = MagicMock()
            mock_afrom_id.return_value = mock_service

            result = await client.aget_dataservice("async_service_id")

            mock_afrom_id.assert_called_once_with("async_service_id", validate=True)
            assert result == mock_service


@pytest.mark.asyncio
async def test_aget_dataservice_with_validate_false():
    """Test async get_dataservice with validate=False."""
    with patch("httpx.Client"):
        client = Fusionbase(api_key="test_api_key")

        with patch.object(DataServiceManager, 'afrom_id', new_callable=AsyncMock) as mock_afrom_id:
            mock_service = MagicMock()
            mock_afrom_id.return_value = mock_service

            await client.aget_dataservice("async_service_id", validate=False)

            mock_afrom_id.assert_called_once_with("async_service_id", validate=False)


if __name__ == "__main__":
    unittest.main()
