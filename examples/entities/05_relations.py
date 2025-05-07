"""
Example demonstrating how to work with relations in the Fusionbase SDK.

This example shows how to fetch and use relation entities, including:
- Getting a relation by ID
- Working with relation properties
- Understanding how relations connect different entity types
"""

import os
import sys
import traceback

from fusionbase import Fusionbase


def main():
    """Run examples of working with relations."""
    print("=== Working with Relations Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Get a relation by ID
        # We'll use network relation for the example
        relation_id = "3138484719"  # Network relation
        print(f"\n1. Getting relation with ID: {relation_id}...")

        relation = client.entities.relations.from_id(relation_id)

        print("\nRelation details:")
        print(f"  ID: {relation.fb_entity_id}")
        print(f"  Name: {relation.relation_name}")
        print(f"  Label: {relation.label}")
        print(
            f"  Description: {relation.relation_description or 'No description'}"
        )

        # Example 2: Understanding relation entity types
        print("\n2. Relation entity types:")
        print(f"  From entity type: {relation.model_from.value}")
        print(f"  To entity type: {relation.model_to.value}")

        # Get Python classes for the entity types
        from_class = relation.get_from_entity_class()
        to_class = relation.get_to_entity_class()

        print(f"  From entity class: {from_class.__name__}")
        print(f"  To entity class: {to_class.__name__}")

        # Example 3: Check if relation can be resolved
        print("\n3. Resolution information:")
        if relation.resolve:
            print("  Can be resolved: Yes")
            if relation.resolve_config.parameter_definition:
                print("  Required parameters:")
                for param in relation.resolve_config.parameter_definition:
                    print(f"    - {param.name} (Required: {param.required})")
            else:
                print("  No parameters required for resolution")

        # Example 4: Inspect metadata
        print("\n4. Relation metadata:")
        if relation.metadata:
            print(f"  Created at: {relation.metadata.get('created_at')}")
            print(f"  Updated at: {relation.metadata.get('updated_at')}")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
