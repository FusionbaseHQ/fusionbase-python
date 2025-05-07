"""
Example demonstrating asynchronous data search with the Fusionbase SDK.

This example shows how to search for data streams and services asynchronously.
"""

import asyncio
import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.data.dataservice import DataService
from fusionbase.data.datastream import DataStream
from fusionbase.search.data_search import DataSearchParams


async def display_entity_details(entity):
    """Display details about a data entity asynchronously."""
    if not entity:
        return False

    if isinstance(entity, DataStream):
        metadata = await entity.aget_metadata()
        print("  Type: DataStream")
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


async def run_data_search():
    """Run asynchronous data searches."""
    print("=== Asynchronous Data Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic async data search
        print("\n1. Basic async data search for 'financial'...")
        params = DataSearchParams(q="financial")
        results = await client.search.data.asearch(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            entity_ref = results.items[0]
            # Explicitly await loading the full entity
            entity = await entity_ref.aget()

            print("\nFirst result:")
            print(f"  ID: {entity_ref.entity_id}")
            await display_entity_details(entity)
        else:
            print("No results found")

        # Example 2: Multiple async searches concurrently
        print("\n2. Running multiple async data searches concurrently")

        # Create search parameters for different topics
        topics = ["climate", "economy", "health", "technology"]
        search_params = [DataSearchParams(q=topic) for topic in topics]

        # Run searches concurrently
        search_tasks = [
            client.search.data.asearch(params) for params in search_params
        ]
        search_results = await asyncio.gather(*search_tasks)

        # Display results for all searches
        for i, (topic, result) in enumerate(zip(topics, search_results)):
            print(f"\nSearch {i+1} for '{topic}':")
            print(f"  Found {len(result.items)} results")

            # Display first result name if available
            if result.items:
                try:
                    entity = await result.items[0].aget()
                    if isinstance(entity, DataStream):
                        metadata = await entity.aget_metadata()
                        print(
                            f"  First result: {metadata.display_name} (DataStream)"
                        )
                    elif isinstance(entity, DataService):
                        print(f"  First result: {entity.name} (DataService)")
                except Exception as e:
                    print(f"  Error loading first result: {e}")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        await client.aclose()
        client.close()


def main():
    """Run the async example."""
    asyncio.run(run_data_search())


if __name__ == "__main__":
    main()
