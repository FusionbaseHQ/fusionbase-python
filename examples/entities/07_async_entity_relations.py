"""
Example demonstrating asynchronous listing of relations available for entity types.

This example shows how to fetch all available relations for different entity types
using asynchronous methods and how to access relation information for a specific entity instance.
"""

import asyncio
import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person


async def run_relations_example():
    """Run asynchronous examples of listing relations."""
    print("=== Asynchronous Listing Relations Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: List relations for multiple entity types concurrently
        print(
            "\n1. Listing relations for multiple entity types concurrently...")

        # Create coroutines for fetching relations of different entity types
        org_relations_task = Organization.alist_relations(client)
        location_relations_task = Location.alist_relations(client)
        person_relations_task = Person.alist_relations(client)

        # Execute them concurrently
        org_relations, location_relations, person_relations = await asyncio.gather(
            org_relations_task, location_relations_task, person_relations_task)

        print(f"Found {len(org_relations)} relations for Organization entity")
        print(f"Found {len(location_relations)} relations for Location entity")
        print(f"Found {len(person_relations)} relations for Person entity")

        # Example 2: Get an entity and its relations asynchronously
        print("\n2. Getting an entity and its relations asynchronously...")

        # Use a known location ID as an example
        location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich
        location = await client.entities.locations.afrom_id(location_id)
        print(f"Got location: {location.formatted_address}")

        # Get relations for this specific location using async method
        location_instance_relations = await location.aget_relations(client)
        print(
            f"Found {len(location_instance_relations)} relations for this location instance"
        )

        # Show some example relations
        for i, relation in enumerate(location_instance_relations[:3],
                                     1):  # Show first 3
            print(f"\n  Relation {i}:")
            print(f"    Name: {relation.relation_name}")
            print(f"    Label: {relation.label}")
            print(f"    From: {relation.model_from.value}")
            print(f"    To: {relation.model_to.value}")

        # Example 3: Try resolving a relation
        print("\n3. Finding a relation that can be resolved...")

        # Find a relation that can be resolved without parameters
        resolvable_relation = None
        for relation in location_instance_relations:
            if relation.resolve and (
                    not relation.resolve.parameter_definition or
                    all(not param.required
                        for param in relation.resolve.parameter_definition)):
                resolvable_relation = relation
                break

        if resolvable_relation:
            print(
                f"  Found resolvable relation: {resolvable_relation.relation_name}"
            )
            print("  Attempting to resolve...")

            try:
                result = await resolvable_relation.aresolve(location)
                print(
                    f"  Resolution successful. Result contains {len(result) if hasattr(result, '__len__') else 'data'}"
                )
            except Exception as e:
                print(f"  Could not resolve relation: {e}")
        else:
            print("  No easily resolvable relations found for this entity")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        await client.aclose()
        client.close()


def main():
    """Run the async example."""
    asyncio.run(run_relations_example())


if __name__ == "__main__":
    main()
