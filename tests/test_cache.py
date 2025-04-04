import time
import unittest

from fusionbase import Fusionbase
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


if __name__ == "__main__":
    unittest.main()
