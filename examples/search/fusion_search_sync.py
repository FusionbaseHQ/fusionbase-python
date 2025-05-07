"""
Example demonstrating synchronous fusion search with the Fusionbase SDK.

This example shows how to search across all entity types with a single query.
"""

import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.search.fusion_search import FusionSearchParams


def display_entity_result(entity_type, entity_data):
    """Display information about an entity result."""
    print(f"  Type: {entity_type}")

    # The structure varies by entity type, but we can handle common patterns
    if isinstance(entity_data, dict):
        if "entity" in entity_data:
            entity = entity_data["entity"]
            print(
                f"  ID: {entity.get('fb_entity_id') or entity.get('key') or entity.get('id', 'Unknown')}"
            )

            # Display name if available
            if "name" in entity:
                if isinstance(entity["name"], dict):
                    name = entity["name"].get("en") or entity["name"].get(
                        "de", "No name")
                else:
                    name = entity["name"]
                print(f"  Name: {name}")
            elif "display_name" in entity:
                print(f"  Name: {entity['display_name']}")

            # Display score if available
            if "score" in entity_data:
                print(f"  Score: {entity_data['score']:.2f}")
        else:
            # Just print some key attributes
            for key, value in entity_data.items():
                if key in ["entity_type", "score", "distance"]:
                    continue
                print(f"  {key}: {value}")


def main():
    """Run synchronous fusion search examples."""
    print("=== Synchronous Fusion Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic fusion search
        query = "health insurance"
        print(f"1. Basic fusion search for '{query}'...")
        params = FusionSearchParams(q=query)
        results = client.search.fusion.search(params)

        # Display knowledge graph if available
        if results.knowledge_graph:
            print("\nKnowledge Graph:")
            print(f"  Intent: {results.knowledge_graph.intent}")
            if results.knowledge_graph.from_entity_type:
                print(
                    f"  From Entity Type: {results.knowledge_graph.from_entity_type}"
                )
            if results.knowledge_graph.from_entity_id:
                print(
                    f"  From Entity ID: {results.knowledge_graph.from_entity_id}"
                )
            if results.knowledge_graph.relation_id:
                print(f"  Relation ID: {results.knowledge_graph.relation_id}")

        # Display search rank order
        print("\nResults Ranking Order:")
        for i, entity_type in enumerate(results.ranks, 1):
            print(f"  {i}. {entity_type}")

        # Display top results by entity type
        print("\nTop Results by Entity Type:")
        total_count = 0

        for entity_type, entities in results.results.items():
            if entities:
                count = len(entities)
                total_count += count
                print(f"\n{entity_type.upper()} ({count} results):")
                # Show top 3 results for each type
                for i, entity in enumerate(entities[:3], 1):
                    print(f"\nResult {i}:")
                    display_entity_result(entity_type, entity)

        print(f"\nTotal results across all entity types: {total_count}")

        # Example 2: Searching for a specific entity
        print("\n2. Searching for a specific company...")
        params = FusionSearchParams(q="Fusionbase GmbH")
        results = client.search.fusion.search(params)

        # Look for organization results
        organizations = results.results.get("organizations", [])
        if organizations:
            print(
                f"Found {len(organizations)} organizations matching the query")
            # Show the top match
            top_org = organizations[0]
            print("\nTop match:")
            display_entity_result("organization", top_org)
        else:
            print("No organization results found")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
