"""Tests for FusionbaseConfig and related configuration classes."""

import os
import tempfile
import unittest

from fusionbase.core.config import CacheConfig
from fusionbase.core.config import FusionbaseConfig
from fusionbase.core.config import LoggingConfig
from fusionbase.core.config import RetryConfig


class TestRetryConfig(unittest.TestCase):
    """Test cases for RetryConfig."""

    def test_retry_config_defaults(self):
        """Test RetryConfig has correct default values."""
        config = RetryConfig()

        self.assertTrue(config.enabled)
        self.assertEqual(config.max_attempts, 5)
        self.assertEqual(config.min_wait_seconds, 1.0)
        self.assertEqual(config.max_wait_seconds, 60.0)
        self.assertEqual(config.retry_statuses, [429, 500, 502, 503, 504])
        self.assertEqual(config.retry_exceptions,
                         ["ConnectionError", "TimeoutError", "ReadTimeout"])

    def test_retry_config_custom_values(self):
        """Test RetryConfig with custom values."""
        config = RetryConfig(enabled=False,
                             max_attempts=3,
                             min_wait_seconds=0.5,
                             max_wait_seconds=30.0,
                             retry_statuses=[500, 503],
                             retry_exceptions=["TimeoutError"])

        self.assertFalse(config.enabled)
        self.assertEqual(config.max_attempts, 3)
        self.assertEqual(config.min_wait_seconds, 0.5)
        self.assertEqual(config.max_wait_seconds, 30.0)
        self.assertEqual(config.retry_statuses, [500, 503])
        self.assertEqual(config.retry_exceptions, ["TimeoutError"])

    def test_retry_config_model_validation(self):
        """Test RetryConfig creation from dict."""
        data = {"enabled": True, "max_attempts": 10, "min_wait_seconds": 2.0}
        config = RetryConfig.model_validate(data)

        self.assertTrue(config.enabled)
        self.assertEqual(config.max_attempts, 10)
        self.assertEqual(config.min_wait_seconds, 2.0)


class TestCacheConfig(unittest.TestCase):
    """Test cases for CacheConfig."""

    def test_cache_config_defaults(self):
        """Test CacheConfig has correct default values."""
        config = CacheConfig()

        self.assertTrue(config.enabled)
        self.assertEqual(config.ttl_seconds, 3600)
        self.assertEqual(config.size_limit, 1_000_000_000)
        self.assertIsNotNone(config.directory)
        self.assertIn("fusionbase-cache", config.directory)

    def test_cache_config_custom_directory(self):
        """Test CacheConfig with custom directory."""
        custom_dir = "/custom/cache/path"
        config = CacheConfig(directory=custom_dir)

        self.assertEqual(config.directory, custom_dir)

    def test_cache_config_disabled(self):
        """Test CacheConfig when disabled."""
        config = CacheConfig(enabled=False)

        self.assertFalse(config.enabled)

    def test_cache_config_custom_ttl(self):
        """Test CacheConfig with custom TTL."""
        config = CacheConfig(ttl_seconds=7200)  # 2 hours

        self.assertEqual(config.ttl_seconds, 7200)

    def test_cache_config_custom_size_limit(self):
        """Test CacheConfig with custom size limit."""
        config = CacheConfig(size_limit=500_000_000)  # 500MB

        self.assertEqual(config.size_limit, 500_000_000)


class TestLoggingConfig(unittest.TestCase):
    """Test cases for LoggingConfig."""

    def test_logging_config_defaults(self):
        """Test LoggingConfig has correct default values."""
        config = LoggingConfig()

        self.assertEqual(config.level, "WARNING")
        self.assertFalse(config.log_requests)
        self.assertFalse(config.log_responses)
        self.assertTrue(config.hide_sensitive_data)
        self.assertIn("{time:", config.format)

    def test_logging_config_debug_level(self):
        """Test LoggingConfig with DEBUG level."""
        config = LoggingConfig(level="DEBUG")

        self.assertEqual(config.level, "DEBUG")

    def test_logging_config_enable_request_logging(self):
        """Test LoggingConfig with request logging enabled."""
        config = LoggingConfig(log_requests=True)

        self.assertTrue(config.log_requests)

    def test_logging_config_show_sensitive_data(self):
        """Test LoggingConfig with sensitive data visible."""
        config = LoggingConfig(hide_sensitive_data=False)

        self.assertFalse(config.hide_sensitive_data)

    def test_logging_config_custom_format(self):
        """Test LoggingConfig with custom format."""
        custom_format = "{level} - {message}"
        config = LoggingConfig(format=custom_format)

        self.assertEqual(config.format, custom_format)


class TestFusionbaseConfig(unittest.TestCase):
    """Test cases for FusionbaseConfig."""

    def test_fusionbase_config_defaults(self):
        """Test FusionbaseConfig has correct default values."""
        config = FusionbaseConfig()

        self.assertEqual(config.timeout, 60.0)
        self.assertFalse(config.async_mode)
        self.assertEqual(config.max_connections, 10)
        self.assertIsInstance(config.cache, CacheConfig)
        self.assertIsInstance(config.retry, RetryConfig)
        self.assertIsInstance(config.logging, LoggingConfig)

    def test_fusionbase_config_custom_timeout(self):
        """Test FusionbaseConfig with custom timeout."""
        config = FusionbaseConfig(timeout=120.0)

        self.assertEqual(config.timeout, 120.0)

    def test_fusionbase_config_async_mode(self):
        """Test FusionbaseConfig with async mode enabled."""
        config = FusionbaseConfig(async_mode=True)

        self.assertTrue(config.async_mode)

    def test_fusionbase_config_custom_max_connections(self):
        """Test FusionbaseConfig with custom max connections."""
        config = FusionbaseConfig(max_connections=20)

        self.assertEqual(config.max_connections, 20)

    def test_fusionbase_config_nested_cache_config(self):
        """Test FusionbaseConfig with nested cache config."""
        config = FusionbaseConfig(
            cache=CacheConfig(enabled=False, ttl_seconds=1800))

        self.assertFalse(config.cache.enabled)
        self.assertEqual(config.cache.ttl_seconds, 1800)

    def test_fusionbase_config_nested_retry_config(self):
        """Test FusionbaseConfig with nested retry config."""
        config = FusionbaseConfig(
            retry=RetryConfig(enabled=False, max_attempts=3))

        self.assertFalse(config.retry.enabled)
        self.assertEqual(config.retry.max_attempts, 3)

    def test_fusionbase_config_from_dict(self):
        """Test FusionbaseConfig with dict values for nested configs."""
        config = FusionbaseConfig(timeout=30.0,
                                  cache={
                                      "enabled": False,
                                      "ttl_seconds": 600
                                  },
                                  retry={"max_attempts": 2})

        self.assertEqual(config.timeout, 30.0)
        self.assertFalse(config.cache.enabled)
        self.assertEqual(config.cache.ttl_seconds, 600)
        self.assertEqual(config.retry.max_attempts, 2)


class TestFusionbaseConfigUpdate(unittest.TestCase):
    """Test cases for FusionbaseConfig.update() method."""

    def test_update_simple_field(self):
        """Test updating a simple field."""
        config = FusionbaseConfig()
        config.update(timeout=90.0)

        self.assertEqual(config.timeout, 90.0)

    def test_update_multiple_fields(self):
        """Test updating multiple fields at once."""
        config = FusionbaseConfig()
        config.update(timeout=45.0, async_mode=True, max_connections=15)

        self.assertEqual(config.timeout, 45.0)
        self.assertTrue(config.async_mode)
        self.assertEqual(config.max_connections, 15)

    def test_update_nested_cache_config(self):
        """Test updating nested cache config via dict."""
        config = FusionbaseConfig()
        config.update(cache={"enabled": False, "ttl_seconds": 300})

        self.assertFalse(config.cache.enabled)
        self.assertEqual(config.cache.ttl_seconds, 300)

    def test_update_nested_retry_config(self):
        """Test updating nested retry config via dict."""
        config = FusionbaseConfig()
        config.update(retry={"enabled": False, "max_attempts": 2})

        self.assertFalse(config.retry.enabled)
        self.assertEqual(config.retry.max_attempts, 2)

    def test_update_nested_logging_config(self):
        """Test updating nested logging config via dict."""
        config = FusionbaseConfig()
        config.update(logging={"level": "DEBUG", "log_requests": True})

        self.assertEqual(config.logging.level, "DEBUG")
        self.assertTrue(config.logging.log_requests)

    def test_update_ignores_unknown_fields(self):
        """Test that update ignores unknown field names."""
        config = FusionbaseConfig()
        original_timeout = config.timeout

        # This should not raise and should not change anything
        config.update(unknown_field="value")

        self.assertEqual(config.timeout, original_timeout)

    def test_update_partial_nested_config(self):
        """Test that partial nested update preserves other values."""
        config = FusionbaseConfig(
            cache=CacheConfig(enabled=True, ttl_seconds=3600))

        # Update only enabled, ttl_seconds should remain
        config.update(cache={"enabled": False})

        self.assertFalse(config.cache.enabled)
        self.assertEqual(config.cache.ttl_seconds, 3600)


class TestConfigIntegration(unittest.TestCase):
    """Integration tests for configuration classes."""

    def test_full_config_creation(self):
        """Test creating a fully customized config."""
        config = FusionbaseConfig(timeout=30.0,
                                  async_mode=True,
                                  max_connections=5,
                                  cache=CacheConfig(enabled=True,
                                                    ttl_seconds=1800,
                                                    size_limit=100_000_000),
                                  retry=RetryConfig(enabled=True,
                                                    max_attempts=3,
                                                    min_wait_seconds=0.5,
                                                    max_wait_seconds=10.0),
                                  logging=LoggingConfig(
                                      level="INFO",
                                      log_requests=True,
                                      log_responses=True,
                                      hide_sensitive_data=True))

        # Verify all settings
        self.assertEqual(config.timeout, 30.0)
        self.assertTrue(config.async_mode)
        self.assertEqual(config.max_connections, 5)

        self.assertTrue(config.cache.enabled)
        self.assertEqual(config.cache.ttl_seconds, 1800)

        self.assertEqual(config.retry.max_attempts, 3)
        self.assertEqual(config.retry.min_wait_seconds, 0.5)

        self.assertEqual(config.logging.level, "INFO")
        self.assertTrue(config.logging.log_requests)

    def test_config_model_dump(self):
        """Test that config can be serialized to dict."""
        config = FusionbaseConfig(timeout=45.0)

        config_dict = config.model_dump()

        self.assertIsInstance(config_dict, dict)
        self.assertEqual(config_dict["timeout"], 45.0)
        self.assertIn("cache", config_dict)
        self.assertIn("retry", config_dict)
        self.assertIn("logging", config_dict)
