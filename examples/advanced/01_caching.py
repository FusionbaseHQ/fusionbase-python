"""
Caching with the Fusionbase SDK.

This example demonstrates how to configure and use the caching system.
"""

import time

from fusionbase import Fusionbase
from fusionbase import FusionbaseConfig
from fusionbase.config import CacheConfig
from fusionbase.logging import configure_logging

# Configure verbose logging to see cache operations
configure_logging(level="INFO")


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def demonstrate_basic_caching(client):
    """Demonstrate basic caching behavior."""
    print_separator("Basic Caching")

    location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich

    print("First request (cache miss expected)...")
    start = time.time()
    location1 = client.entities.locations.from_id(location_id)
    duration1 = time.time() - start
    print(f"Retrieved {location1.formatted_address} in {duration1:.4f} seconds")

    print("\nSecond request (cache hit expected)...")
    start = time.time()
    location2 = client.entities.locations.from_id(location_id)
    duration2 = time.time() - start
    print(f"Retrieved {location2.formatted_address} in {duration2:.4f} seconds")

    if duration1 > duration2:
        speedup = duration1 / duration2
        print(f"✓ Cache is working! {speedup:.1f}x faster with cache")
    else:
        print(
            "⚠️ Cache might not be working as expected (second request wasn't faster)"
        )


def demonstrate_cache_control(client: Fusionbase):
    """Demonstrate manual cache control."""
    print_separator("Manual Cache Control")

    if not client.config.cache.enabled:
        print("⚠️ Caching is not enabled for this client")
        return

    location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich

    # First, ensure the location is cached
    print("Ensuring location is cached...")
    location = client.entities.locations.from_id(location_id)
    print(f"Location cached: {location.formatted_address}")

    # Clear the entire cache
    print("\nClearing entire cache...")
    client._cache.clear()  # pylint: disable=protected-access
    print("✓ Cache cleared")

    # Verify we get a cache miss
    print("\nNext request (cache miss expected after clear)...")
    start = time.time()
    location = client.entities.locations.from_id(location_id)
    duration = time.time() - start
    print(f"Retrieved {location.formatted_address} in {duration:.4f} seconds")


def main():
    """Run the caching examples."""
    try:
        # Configure a client with caching enabled
        config = FusionbaseConfig(cache=CacheConfig(
            enabled=True,
            ttl_seconds=300,  # 5 minutes
            size_limit=10_000_000  # ~10MB
        ))

        with Fusionbase(config=config) as client:
            demonstrate_basic_caching(client)
            demonstrate_cache_control(client)

            print_separator("Caching Examples Complete")

    except ValueError as e:
        print(f"Error initializing client: {e}")
        print("Make sure to set the FUSIONBASE_API_KEY environment variable.")


if __name__ == "__main__":
    main()
