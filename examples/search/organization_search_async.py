"""
Example demonstrating asynchronous organization search with the Fusionbase SDK.

This example shows how to search for organizations asynchronously.
"""

import asyncio
import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.search.organization_search import OrganizationSearchParams


async def run_organization_search():
    """Run asynchronous organization searches."""
    print("=== Asynchronous Organization Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client - no need for separate async client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic async organization search
        print("\n1. Basic async organization search for 'OroraTech'...")
        params = OrganizationSearchParams(q="OroraTech")
        results = await client.search.organizations.asearch(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            org_ref = results.items[0]
            # Explicitly await loading the full organization
            org = await org_ref.aget()

            print("\nFirst result:")
            print(f"  ID: {org.fb_entity_id}")
            print(f"  Name: {org.name}")
            if org.status:
                print(f"  Status: {org.status.status}")
                print(f"  Active: {org.is_active}")
            if org.address:
                print(f"  Address: {org.address.formatted_address}")
        else:
            print("No results found")

        # Example 2: Multiple async searches concurrently
        print("\n2. Running multiple async organization searches concurrently")

        # Create search parameters for different queries
        queries = ["Tech", "Software", "AI", "GmbH"]
        search_params = [
            OrganizationSearchParams(q=query, limit=3) for query in queries
        ]

        # Run searches concurrently
        search_tasks = [
            client.search.organizations.asearch(params)
            for params in search_params
        ]
        search_results = await asyncio.gather(*search_tasks)

        # Display results for all searches
        for i, (query, result) in enumerate(zip(queries, search_results)):
            print(f"\nSearch {i+1} for '{query}':")
            print(
                f"  Found {len(result.items)} results (out of {result.total} total)"
            )

            # Display first result name if available
            if result.items:
                # Access entity name without loading full entity
                org = await result.items[0].aget()
                print(f"  First result: {org.name}")

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
    asyncio.run(run_organization_search())


if __name__ == "__main__":
    main()
