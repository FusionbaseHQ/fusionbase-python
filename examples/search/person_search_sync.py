"""
Example demonstrating synchronous person search with the Fusionbase SDK.

This example shows how to search for persons and handle the search results.
"""
# pylint: disable=duplicate-code  # Similarity with async example is intentional

import os
import sys
import traceback  # Removed unused asyncio import

from fusionbase import Fusionbase
from fusionbase.search.person_search import PersonSearchParams


def safely_get_person(person_ref):
    """Safely retrieve a person from a reference with error handling."""
    try:
        return person_ref.get()
    except Exception as e:
        print(f"  Error loading person: {e}")
        return None


def display_person_details(person):
    """Display details about a person."""
    if not person:
        return False

    print("\nFirst result:")
    print(f"  ID: {person.fb_entity_id}")
    print(f"  Name: {person.given_name} {person.family_name}")

    if person.birth_date and hasattr(person.birth_date,
                                     'value') and person.birth_date.value:
        print(f"  Birth Date: {person.birth_date.value.strftime('%Y-%m-%d')}")

    # Display home location if available
    if person.home_location:
        print(f"  Home Location: {person.home_location.formatted_address}")
    else:
        print("  No home location available")
    return True


def main():  # pylint: disable=too-many-statements
    """Run synchronous person search examples."""
    print("=== Synchronous Person Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic person search
        print("1. Basic person search for 'Kevin Goßling' (example)...")
        params = PersonSearchParams(q="Kevin Goßling")
        results = client.search.persons.search(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            person_ref = results.items[0]
            # Get the person from LazyReference with error handling
            person = safely_get_person(person_ref)
            if not display_person_details(person):
                print("Could not display person details")
        else:
            print("No results found")

        # Example 2: Search with source filtering
        print("\n2. Search for persons from German Business Registry...")
        params = PersonSearchParams(
            q="Müller",
            source_key='1051122944',
        )
        results = client.search.persons.search(params)
        print(
            f"Found {len(results.items)} results from German Business Registry")

        # Example 3: Safely examining location data
        print("\n3. Examining location data example...")
        if results.items:
            person_ref = results.items[0]

            # Get person ID safely without triggering loading
            person_id = person_ref.entity_id
            print(f"  Getting sample person with ID: {person_id}")

            try:
                # Explicitly load person with full error handling
                person = person_ref.get()
                print(f"  Name: {person.given_name} {person.family_name}")

                # Safely check locations
                if hasattr(person, 'locations') and person.locations:
                    print("  Location data:")
                    for key, loc in person.locations.items():
                        if loc is None:
                            print(f"    {key}: None")
                        elif hasattr(loc, 'formatted_address'):
                            print(f"    {key}: {loc.formatted_address}")
                        else:
                            print(f"    {key}: {loc}")
                else:
                    print("  No location data available for this person")
            except Exception as e:
                print(f"  Error accessing person data: {e}")
                traceback.print_exc(limit=2)

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
