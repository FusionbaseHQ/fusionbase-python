"""
Example demonstrating asynchronous person search with the Fusionbase SDK.

This example shows how to search for persons asynchronously.
"""
# pylint: disable=duplicate-code  # Similarity with sync example is intentional

import asyncio
import os
import sys

from fusionbase import Fusionbase
from fusionbase.entities.person import Source
from fusionbase.search.person_search import PersonSearchParams


async def run_person_search():
    """Run asynchronous person searches."""
    print("=== Asynchronous Person Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client and get the async client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic async person search
        print("\n1. Basic async person search for 'Kevin Goßling' (example)...")
        params = PersonSearchParams(q="Kevin Goßling")
        results = await client.search.persons.asearch(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            # Get LazyReference to person
            person_ref = results.items[0]
            # Explicitly await loading the full person
            person = await person_ref.aget()

            print("\nFirst result:")
            print(f"  ID: {person.fb_entity_id}")
            print(f"  Name: {person.given_name} {person.family_name}")

            # Display home location if available
            if person.home_location:
                print(
                    f"  Home Location: {person.home_location.formatted_address}"
                )
            else:
                print("  No home location available")
        else:
            print("No results found")

        # Example 2: Parallel searches across different business registries
        print("\n2. Parallel searches across different business registries...")

        # Create search tasks for different registries
        german_params = PersonSearchParams(
            q="Müller",
            source_key=Source.GERMAN_BUSINESS_REGISTRY.rsplit(
                '/', maxsplit=1)[-1],  # Fixed rsplit
            limit=10)
        uk_params = PersonSearchParams(
            q="Smith",
            source_key=Source.UK_BUSINESS_REGISTRY.rsplit(
                '/', maxsplit=1)[-1],  # Fixed rsplit
            limit=10)

        # Execute both searches in parallel
        german_task = client.search.persons.asearch(german_params)
        uk_task = client.search.persons.asearch(uk_params)

        # Await results
        german_results, uk_results = await asyncio.gather(german_task, uk_task)

        print(
            f"  German registry search found {len(german_results.items)} results"
        )
        print(f"  UK registry search found {len(uk_results.items)} results")

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close client to clean up resources
        if hasattr(client, 'async_client'):
            await client.async_client.aclose()  # Fixed unused expression
        client.close()


def main():
    """Entry point for script."""
    asyncio.run(run_person_search())


if __name__ == "__main__":
    main()
