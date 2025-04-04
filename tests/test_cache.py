"""Tests for the cache functionality."""

import os
from tempfile import TemporaryDirectory
import time
import unittest

import pytest

from fusionbase import Fusionbase
from fusionbase.cache import FusionbaseCache
from fusionbase.config import CacheConfig
from fusionbase.config import FusionbaseConfig


class TestFusionbaseCache(unittest.TestCase):

    def test_caching(self):
        config = FusionbaseConfig(
            cache=CacheConfig(enabled=True, ttl_seconds=10))
        with Fusionbase(config=config) as client:
            # First request (cache miss)
            start = time.time()
            loc1 = client.entities.locations.from_id(
                "bfcc19ddd9edb12efb9cfea181b0dcd3")
            miss_duration = time.time() - start

            # Second request (cache hit expected)
            start = time.time()
            loc2 = client.entities.locations.from_id(
                "bfcc19ddd9edb12efb9cfea181b0dcd3")
            hit_duration = time.time() - start

            self.assertEqual(loc1.fb_entity_id, loc2.fb_entity_id)
            self.assertLess(hit_duration, miss_duration,
                            "Cache did not speed up retrieval")


class TestCache(unittest.TestCase):
    """Test cache functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.cache = FusionbaseCache(
            cache_dir=self.temp_dir.name,
            ttl=60,
            enabled=True,
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.cache.close()
        self.temp_dir.cleanup()

    def test_cache_get_set(self):
        """Test basic cache set and get operations."""
        # Set a value
        self.assertTrue(self.cache.set("test_key", "test_value"))

        # Get the value
        self.assertEqual(self.cache.get("test_key"), "test_value")

        # Get a non-existent key
        self.assertIsNone(self.cache.get("non_existent"))

    def test_cache_clear(self):
        """Test cache clear operation."""
        # Set values
        self.cache.set("key1", "value1")
        self.cache.set("key2", "value2")

        # Clear cache
        self.assertTrue(self.cache.clear())

        # Check values are gone
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))

    def test_cache_invalidate(self):
        """Test cache invalidation by prefix."""
        # Set values
        self.cache.set("prefix1:key1", "value1")
        self.cache.set("prefix1:key2", "value2")
        self.cache.set("prefix2:key1", "value3")

        # Invalidate by prefix
        count = self.cache.invalidate("prefix1:")

        # Check appropriate values are gone
        self.assertEqual(count, 2)
        self.assertIsNone(self.cache.get("prefix1:key1"))
        self.assertIsNone(self.cache.get("prefix1:key2"))
        self.assertEqual(self.cache.get("prefix2:key1"), "value3")


@pytest.mark.asyncio
async def test_client_caching():
    """Test caching in the client."""
    # Use an API key from environment or skip
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create a client with caching enabled
    config = FusionbaseConfig(cache={"enabled": True, "ttl_seconds": 60})
    client = Fusionbase(api_key=api_key, config=config)

    try:
        # Make a request that should be cached
        result1 = client.request("GET",
                                 "search/entities/person",
                                 params={
                                     "q": "test",
                                     "limit": 1
                                 })

        # Make the same request again, should use cache
        result2 = client.request("GET",
                                 "search/entities/person",
                                 params={
                                     "q": "test",
                                     "limit": 1
                                 })

        # Results should be identical
        assert result1 == result2

        # Check if the async client also uses cache
        async_client = client.async_client

        # Make the same request asynchronously, should use cache
        result3 = await async_client.request("GET",
                                             "search/entities/person",
                                             params={
                                                 "q": "test",
                                                 "limit": 1
                                             })

        # Result should be identical to previous
        assert result1 == result3

    finally:
        # Clean up
        client.close()


if __name__ == "__main__":
    unittest.main()
