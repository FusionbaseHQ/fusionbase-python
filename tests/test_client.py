"""Tests for the Fusionbase client."""

import os
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from fusionbase import Fusionbase
from fusionbase.config import FusionbaseConfig
from fusionbase.config import RetryConfig


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
        # copy if in environment
        api_key_cache = os.getenv("FUSIONBASE_API_KEY")
        # remove from environment
        del os.environ["FUSIONBASE_API_KEY"]
        with self.assertRaises(ValueError):
            Fusionbase(api_key=None)
        # restore environment variable
        if api_key_cache is not None:
            os.environ["FUSIONBASE_API_KEY"] = api_key_cache
        else:
            del os.environ["FUSIONBASE_API_KEY"]

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


if __name__ == "__main__":
    unittest.main()
