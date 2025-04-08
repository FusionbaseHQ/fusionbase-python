"""
Example demonstrating asynchronous location search with the Fusionbase SDK.

This example shows how to search for locations using async methods.
"""

import asyncio
import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.search.location_search import LocationSearchParams


async def run_location_search():
    """Run asynchronous location searches."""
    print("=== Asynchronous Location Search Example ===")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client - use async methods directly
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic async location search
        print("\n1. Basic async location search for "
              "'Munich, Germany' (example)...")
        params = LocationSearchParams(q="Munich, Germany")
        results = await client.search.locations.asearch(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            location = results.items[0]
            print("\nFirst result:")
            print(f"  ID: {location.fb_entity_id}")
            print(f"  Address: {location.formatted_address}")
            print(
                f"  Coordinates: {location.coordinate.latitude}, {location.coordinate.longitude}"
            )
            print(f"  City: {location.city}")
            print(f"  State: {location.state}")
            print(f"  Country: {location.country}")

        # Example 2: Multiple async searches concurrently
        print("\n2. Running multiple async location searches concurrently")

        # Create search parameters for different cities
        cities = ["Berlin", "Hamburg", "Frankfurt", "Cologne"]
        search_params = [LocationSearchParams(q=city) for city in cities]

        # Run searches concurrently
        search_tasks = [
            client.search.locations.asearch(params) for params in search_params
        ]
        search_results = await asyncio.gather(*search_tasks)

        # Display results for all searches
        for _, (city, result) in enumerate(zip(cities, search_results)):
            if result.items:
                location = result.items[0]
                print(f"\nResult for {city}:")
                print(f"  Address: {location.formatted_address}")
            else:
                print(f"\nNo results found for {city}")

    except OSError as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Always close the client
        await client.aclose()
        client.close()


def main():
    """Run the async example."""
    asyncio.run(run_location_search())


if __name__ == "__main__":
    main()
