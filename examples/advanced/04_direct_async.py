"""
Direct Async API in Fusionbase SDK.

This example demonstrates how to use the direct async methods on the main client
"""

import asyncio
import os
import time
from typing import List

from fusionbase import Fusionbase
from fusionbase.entities.location import Location
from fusionbase.search.location_search import LocationSearchParams


async def fetch_location(client, location_id: str) -> Location:
    """Fetch a location using the direct async API.

    Args:
        client: Fusionbase client
        location_id: ID of the location to fetch

    Returns:
        Location entity
    """
    return await client.entities.locations.afrom_id(location_id)


async def search_locations(client, query: str) -> List[Location]:
    """Search for locations using the direct async API.

    Args:
        client: Fusionbase client
        query: Search query

    Returns:
        List of locations
    """
    params = LocationSearchParams(q=query, limit=5)
    results = await client.search.locations.asearch(params)

    # Load all locations in parallel
    locations = await asyncio.gather(*[item.aget() for item in results.items])

    return locations


async def make_direct_request(client, query: str):
    """Make a direct async request to the API.

    Args:
        client: Fusionbase client
        query: Search query

    Returns:
        Raw API response
    """
    return await client.aget("search/entities/location",
                             params={
                                 "q": query,
                                 "limit": 3
                             })


async def main():
    """Run the direct async example."""
    print("=== Direct Async API Example ===\n")

    # Get API key from environment
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("Error: FUSIONBASE_API_KEY environment variable not set")
        return

    # Create a single client for both sync and async operations
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Fetch a location async
        print("1. Fetching Munich location asynchronously...")
        start = time.time()
        munich = await fetch_location(client,
                                      "bfcc19ddd9edb12efb9cfea181b0dcd3")
        duration = time.time() - start
        print(
            f"✓ Location fetched in {duration:.4f}s: {munich.formatted_address}"
        )

        # Example 2: Search for locations async
        print("\n2. Searching for Berlin locations asynchronously...")
        start = time.time()
        berlin_locations = await search_locations(client, "Berlin")
        duration = time.time() - start
        print(f"✓ Found {len(berlin_locations)} locations in {duration:.4f}s")

        # Print location details
        for i, location in enumerate(berlin_locations, 1):
            print(f"  {i}. {location.formatted_address}")

        # Example 3: Direct API request
        print("\n3. Making a direct async API request...")
        start = time.time()
        response = await make_direct_request(client, "Frankfurt")
        duration = time.time() - start
        print(f"✓ API request completed in {duration:.4f}s")
        print(f"  Total results: {response.get('total', 0)}")

        # Example 4: Async context manager
        print("\n4. Using the client as an async context manager...")
        start = time.time()
        async with Fusionbase(api_key=api_key) as ctx_client:
            hamburg = await ctx_client.entities.locations.afrom_id(
                "7835fb9a2ece0a6685d056c43c1e36f7")
            print(f"  Found location: {hamburg.formatted_address}")
        duration = time.time() - start
        print(f"✓ Async context manager used in {duration:.4f}s")

    finally:
        # Always close both sync and async resources
        await client.aclose()
        client.close()
        print("\nExample complete.")


if __name__ == "__main__":
    asyncio.run(main())
