"""
Example demonstrating asynchronous relation search with the Fusionbase SDK.

This example shows how to search for relations asynchronously.
"""

import asyncio
import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.search.relation_search import RelationSearchParams


async def run_relation_search():
    """Run asynchronous relation searches."""
    print("=== Asynchronous Relation Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client - no need for separate async client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic async relation search
        print("\n1. Basic async relation search for 'network'...")
        params = RelationSearchParams(q="network")
        results = await client.search.relations.asearch(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            relation_ref = results.items[0]
            # Explicitly await loading the full relation
            relation = await relation_ref.aget()

            print("\nFirst result:")
            print(f"  ID: {relation.fb_entity_id}")
            print(f"  Name: {relation.relation_name}")
            print(f"  Label: {relation.label}")
            print(f"  From entity type: {relation.model_from.value}")
            print(f"  To entity type: {relation.model_to.value}")
            print(
                f"  Description: {relation.relation_description or 'No description'}"
            )
        else:
            print("No results found")

        # Example 2: Multiple async searches concurrently
        print("\n2. Running multiple async relation searches concurrently")

        # Create search parameters for different queries
        queries = ["network", "location", "organization", "dashboard"]
        search_params = [
            RelationSearchParams(q=query, limit=3) for query in queries
        ]

        # Run searches concurrently
        search_tasks = [
            client.search.relations.asearch(params) for params in search_params
        ]
        search_results = await asyncio.gather(*search_tasks)

        # Display results for all searches
        for i, (query, result) in enumerate(zip(queries, search_results)):
            print(f"\nSearch {i+1} for '{query}':")
            print(f"  Found {len(result.items)} results")

            # Display first result name if available
            if result.items:
                # Access relation name by loading the full entity
                relation = await result.items[0].aget()
                print(f"  First result: {relation.relation_name}")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Always close the client
        await client.aclose()
        client.close()


def main():
    """Run the async example."""
    asyncio.run(run_relation_search())


if __name__ == "__main__":
    main()
