"""
Example demonstrating synchronous data search with the Fusionbase SDK.

This example shows how to search for data streams and services.
"""

import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.data.dataservice import DataService
from fusionbase.data.datastream import DataStream
from fusionbase.search.data_search import DataSearchParams


def safely_get_data_entity(entity_ref):
    """Safely retrieve a data entity from a reference with error handling."""
    try:
        return entity_ref.get()
    except Exception as e:
        print(f"  Error loading entity: {e}")
        return None


def display_entity_details(entity):
    """Display details about a data entity."""
    if not entity:
        return False

    if isinstance(entity, DataStream):
        print("  Type: DataStream")
        metadata = entity.get_metadata()
        print(f"  Name: {metadata.display_name}")
        print(
            f"  Description: {metadata.description.en if metadata.description and metadata.description.en else 'No description'}"
        )
        print(f"  Record count: {metadata.meta.entry_count}")
        print(f"  Column count: {len(metadata.data_item_collections)}")
    elif isinstance(entity, DataService):
        print("  Type: DataService")
        print(f"  Name: {entity.name}")
        print(f"  Description: {entity.description}")
    else:
        print(f"  Unknown entity type: {type(entity)}")

    return True


def main():
    """Run synchronous data search examples."""
    print("=== Synchronous Data Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic search for streams and services
        print("1. Basic data search for 'financial'...")
        params = DataSearchParams(q="financial")
        results = client.search.data.search(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            entity_ref = results.items[0]
            entity = safely_get_data_entity(entity_ref)

            print("\nFirst result:")
            print(f"  ID: {entity_ref.entity_id}")
            display_entity_details(entity)
        else:
            print("No results found")

        # Example 2: Searching specifically for data related to a topic
        print("\n2. Searching for data related to 'weather'...")
        results = client.search.data.search(q="weather")
        print(f"Found {len(results.items)} results")

        # Show first few results if any
        if results.items:
            print("\nTop results:")
            for i, entity_ref in enumerate(results.items[:3]):
                print(f"\nResult {i+1}:")
                print(f"  ID: {entity_ref.entity_id}")

                # Try to load and display entity details
                try:
                    entity = entity_ref.get()
                    display_entity_details(entity)
                except Exception as e:
                    print(f"  Error loading entity: {e}")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
