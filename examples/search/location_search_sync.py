"""
Example demonstrating synchronous location search with the Fusionbase SDK.

This example shows how to search for locations and handle the search results.
"""

import os
import sys

from fusionbase import Fusionbase
from fusionbase.search.location_search import LocationSearchParams


def main():
    """Run synchronous location search examples."""
    print("=== Synchronous Location Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic location search
        print("\n1. Basic location search for 'Munich, Germany'")
        params = LocationSearchParams(q="Munich, Germany")
        results = client.search.locations.search(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            location = results.items[0]
            print("\nFirst result:")
            print(f"  ID: {location.fb_entity_id}")
            print(f"  Address: {location.formatted_address}")
            print(
                "  Coordinates: "
                f"{location.coordinate.latitude}, {location.coordinate.longitude}"
            )
            print(f"  City: {location.city}")
            print(f"  State: {location.state}")
            print(f"  Country: {location.country}")

        # Example 2: Location search with limit
        print("\n2. Limited location search for 'Berlin' (max 1 result)")
        params = LocationSearchParams(q="Berlin", limit=1)
        results = client.search.locations.search(params)
        print(f"Found {len(results.items)} results (limit was {params.limit})")

        # Example 3: Searching for a specific address
        print("\n3. Search for a specific address")
        params = LocationSearchParams(q="Agnes-Pockels-Bogen 1, 80992 München")
        results = client.search.locations.search(params)

        if results.items:
            location = results.items[0]
            print("\nFound address:")
            print(f"  ID: {location.fb_entity_id}")
            print(f"  Address: {location.formatted_address}")
            print(
                "  Coordinates: "
                f"{location.coordinate.latitude}, {location.coordinate.longitude}"
            )

            # Print address components
            print("\nAddress components:")
            for component in location.address_components:
                print(
                    f"  {component.component_type.value}: {component.component_value}"
                )

    except (ValueError, ConnectionError) as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
