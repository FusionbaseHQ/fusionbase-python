"""
Example demonstrating how to resolve relations between entities.

This example shows how to use relations to navigate between connected entities
and how to resolve relations with and without parameters.
"""

import asyncio
import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference


def display_relation_result(result, indent="  "):
    """Display relation resolution results in a readable format."""
    if isinstance(result, list):
        print(f"{indent}List with {len(result)} items:")
        for i, item in enumerate(result[:3], 1):  # Show first 3
            if isinstance(item, LazyReference):
                entity_type = type(item._entity_class).__name__
                print(
                    f"{indent}  {i}. LazyReference to {entity_type}: {item.entity_id}"
                )
            else:
                print(f"{indent}  {i}. {type(item).__name__}: {str(item)[:50]}")
        if len(result) > 3:
            print(f"{indent}  ... and {len(result) - 3} more items")
    elif isinstance(result, dict):
        print(f"{indent}Dictionary with {len(result)} keys:")
        for i, (key, value) in enumerate(list(result.items())[:3],
                                         1):  # Show first 3
            print(f"{indent}  {i}. {key}: {type(value).__name__}")
        if len(result) > 3:
            print(f"{indent}  ... and {len(result) - 3} more items")
    elif isinstance(result, LazyReference):
        entity_type = type(result._entity_class).__name__
        print(f"{indent}LazyReference to {entity_type}: {result.entity_id}")
    else:
        print(f"{indent}Result type: {type(result).__name__}")
        print(f"{indent}Value: {str(result)[:100]}")


async def main():
    """Run examples of resolving relations."""
    print("=== Resolving Relations Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # We'll use Munich as our example entity
        location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich
        print(f"\n1. Starting with location ID: {location_id}...")

        location = await client.entities.locations.afrom_id(location_id)
        print(f"Got location: {location.formatted_address}")

        # Get relations for this location
        relations = await location.aget_relations(client)
        print(f"Found {len(relations)} relations for this location")

        # Find a relation that doesn't require parameters
        simple_relation = None
        for relation in relations:
            if relation.resolve and (
                    not relation.resolve.parameter_definition or
                    all(not param.required
                        for param in relation.resolve.parameter_definition)):
                simple_relation = relation
                break

        if simple_relation:
            print(
                f"\n2. Found relation without required parameters: {simple_relation.relation_name}"
            )
            print(
                f"   From: {simple_relation.model_from.value} -> To: {simple_relation.model_to.value}"
            )

            try:
                print("\n   Resolving relation...")
                result = await simple_relation.aresolve(location)

                print("   Resolution successful.")
                display_relation_result(result)

                # If the result is a list of LazyReferences, let's load the first one
                if isinstance(result, list) and result and isinstance(
                        result[0], LazyReference):
                    print("\n   Loading the first entity from the result...")
                    entity = await result[0].aget()
                    print(
                        f"   Loaded entity: {entity.name if hasattr(entity, 'name') and entity.name else entity.fb_entity_id}"
                    )

            except Exception as e:
                print(f"   Could not resolve relation: {e}")
        else:
            print("\n2. No relations without required parameters found")

        # Find a relation that requires parameters
        print("\n3. Looking for a relation that requires parameters...")

        parameterized_relation = None
        for relation in relations:
            if relation.resolve and relation.resolve.parameter_definition and any(
                    param.required
                    for param in relation.resolve.parameter_definition):
                parameterized_relation = relation
                break

        if parameterized_relation:
            print(
                f"   Found relation with parameters: {parameterized_relation.relation_name}"
            )
            print(
                f"   From: {parameterized_relation.model_from.value} -> To: {parameterized_relation.model_to.value}"
            )

            print("\n   Required parameters:")
            for param in parameterized_relation.resolve.parameter_definition:
                print(
                    f"     - {param.name} (Required: {param.required}, Type: {param.param_type if hasattr(param, 'param_type') else 'unknown'})"
                )

            print(
                "\n   To resolve this relation, you would need to provide these parameters:"
            )
            print("   # Example (pseudocode):")
            params_example = {
                param.name: "value"
                for param in parameterized_relation.resolve.parameter_definition
                if param.required
            }
            print(
                f"   result = await relation.aresolve(entity, parameters={params_example})"
            )

        else:
            print("   No relations with required parameters found")

        # Search for an organization and show its relations
        print(
            "\n4. Searching for an organization and checking its relations...")

        try:
            search_results = await client.search.organizations.asearch(
                q="OroraTech", limit=1)

            if search_results.items:
                org = await search_results.items[0].aget()
                print(f"   Found organization: {org.name}")

                # Get relations for this organization
                org_relations = await org.aget_relations(client)
                print(
                    f"   This organization has {len(org_relations)} available relations"
                )

                # Show some examples
                for i, relation in enumerate(org_relations[:3], 1):
                    print(f"\n   Relation {i}: {relation.relation_name}")
                    print(f"     Label: {relation.label}")
                    print(
                        f"     From: {relation.model_from.value} -> To: {relation.model_to.value}"
                    )

                if len(org_relations) > 3:
                    print(
                        f"\n   ... and {len(org_relations) - 3} more relations")

            else:
                print("   No organizations found in the search")

        except Exception as e:
            print(f"   Error during organization search: {e}")

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        await client.aclose()
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
