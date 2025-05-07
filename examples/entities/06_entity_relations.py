"""
Example demonstrating how to list relations available for entity types.

This example shows how to fetch all available relations for different entity types
and how to access relation information for a specific entity instance.
"""

import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person


def main():
    """Run examples of listing relations."""
    print("=== Listing Relations Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: List relations for Organization entity type
        print("\n1. Listing relations for Organization entity type...")
        org_relations = Organization.list_relations(client)
        print(f"Found {len(org_relations)} relations for Organization entity")

        # Display some relations
        for i, relation in enumerate(org_relations[:3], 1):  # Show first 3
            print(f"\n  Relation {i}:")
            print(f"    Name: {relation.relation_name}")
            print(f"    Label: {relation.label}")
            print(f"    From: {relation.model_from.value}")
            print(f"    To: {relation.model_to.value}")

        if len(org_relations) > 3:
            print(f"\n  ... and {len(org_relations) - 3} more relations")

        # Example 2: List relations for Location entity type
        print("\n2. Listing relations for Location entity type...")
        location_relations = Location.list_relations(client)
        print(f"Found {len(location_relations)} relations for Location entity")

        # Display some relations
        for i, relation in enumerate(location_relations[:3], 1):  # Show first 3
            print(f"\n  Relation {i}:")
            print(f"    Name: {relation.relation_name}")
            print(f"    Label: {relation.label}")
            print(f"    From: {relation.model_from.value}")
            print(f"    To: {relation.model_to.value}")

        if len(location_relations) > 3:
            print(f"\n  ... and {len(location_relations) - 3} more relations")

        # Example 3: List relations for Person entity type
        print("\n3. Listing relations for Person entity type...")
        person_relations = Person.list_relations(client)
        print(f"Found {len(person_relations)} relations for Person entity")

        # Display some relations
        for i, relation in enumerate(person_relations[:3], 1):  # Show first 3
            print(f"\n  Relation {i}:")
            print(f"    Name: {relation.relation_name}")
            print(f"    Label: {relation.label}")
            print(f"    From: {relation.model_from.value}")
            print(f"    To: {relation.model_to.value}")

        if len(person_relations) > 3:
            print(f"\n  ... and {len(person_relations) - 3} more relations")

        # Example 4: Get relations for a specific entity instance
        print("\n4. Getting relations for a specific entity instance...")

        # Use a known location ID as an example
        location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich
        location = client.entities.locations.from_id(location_id)
        print(f"Got location: {location.formatted_address}")

        # Get relations for this specific location
        location_instance_relations = location.get_relations(client)
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

            # Check if relation can be resolved and show parameter info
            if relation.resolve_config:
                if relation.resolve_config.parameter_definition:
                    params = ', '.join(
                        [p.name for p in relation.resolve_config.parameter_definition])
                    print(f"    Parameters: {params}")
                else:
                    print("    No parameters required")
            else:
                print("    Cannot be resolved")

        if len(location_instance_relations) > 3:
            print(
                f"\n  ... and {len(location_instance_relations) - 3} more relations"
            )

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
