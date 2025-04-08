"""
Example demonstrating synchronous relation search with the Fusionbase SDK.

This example shows how to search for relations and handle the search results.
"""

import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.search.relation_search import RelationSearchParams


def safely_get_relation(relation_ref):
    """Safely retrieve a relation from a reference with error handling."""
    try:
        return relation_ref.get()
    except Exception as e:
        print(f"  Error loading relation: {e}")
        return None


def display_relation_details(relation):
    """Display details about a relation."""
    if not relation:
        return False

    print("\nRelation details:")
    print(f"  ID: {relation.fb_entity_id}")
    print(f"  Name: {relation.relation_name}")
    print(f"  Label: {relation.label}")
    print(f"  Description: {relation.relation_description or 'No description'}")
    print(f"  From entity type: {relation.model_from.value}")
    print(f"  To entity type: {relation.model_to.value}")

    # Display resolution info if available
    if relation.resolve:
        print("  Can be resolved: Yes")
        if relation.resolve.parameter_definition:
            print("  Required parameters:")
            for param in relation.resolve.parameter_definition:
                print(f"    - {param.name} (Required: {param.required})")
    else:
        print("  Can be resolved: No")

    return True


def main():
    """Run synchronous relation search examples."""
    print("=== Synchronous Relation Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic relation search
        print("1. Basic relation search for 'network'...")
        params = RelationSearchParams(q="network")
        results = client.search.relations.search(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            relation_ref = results.items[0]
            # Get the relation from LazyReference with error handling
            relation = safely_get_relation(relation_ref)
            if not display_relation_details(relation):
                print("Could not display relation details")
        else:
            print("No results found")

        # Example 2: Search with limit
        print("\n2. Search for relations with limit...")
        params = RelationSearchParams(q="location", limit=3)
        results = client.search.relations.search(params)
        print(f"Found {len(results.items)} results (limited to {params.limit})")

        # Example 3: Pagination example
        print("\n3. Pagination example...")
        first_page = client.search.relations.search(q="organization",
                                                    limit=2,
                                                    skip=0)
        second_page = client.search.relations.search(q="organization",
                                                     limit=2,
                                                     skip=2)

        print(f"First page results: {len(first_page.items)}")
        print(f"Second page results: {len(second_page.items)}")

        if first_page.items and second_page.items:
            # Verify we have different results
            first_id = first_page.items[0].entity_id
            second_id = second_page.items[0].entity_id
            print(f"First page first item ID: {first_id}")
            print(f"Second page first item ID: {second_id}")
            print(f"Items are different: {first_id != second_id}")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
