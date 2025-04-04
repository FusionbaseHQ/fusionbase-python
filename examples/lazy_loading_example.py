"""Example showing lazy loading of entities from search results."""

import asyncio
import os
from typing import List

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.person import Person
from fusionbase.search.person_search import PersonSearchParams


async def main():
    """Run the example."""
    # Initialize Fusionbase client
    fb = Fusionbase(api_key=os.environ.get("FUSIONBASE_API_KEY"))

    # Search for persons with lazy loading
    print("Searching for persons...")
    search_params = PersonSearchParams(q="John", limit=5)
    results = await fb.search.persons.asearch(search_params)

    print(f"Found {results.total} persons")

    # The search results contain LazyReference objects that only load the entity when accessed
    persons: List[LazyReference[Person]] = results.items

    # Accessing properties will trigger loading of the entity
    for i, person_ref in enumerate(
            persons[:2]):  # Only look at first 2 for brevity
        print(f"\nPerson {i+1} (ID: {person_ref.entity_id}):")

        # This will load the entity through the GET endpoint
        print(f"  Name: {person_ref.given_name} {person_ref.family_name}")

        # The entity is now loaded, further access doesn't trigger API calls
        print(f"  Entity loaded: {person_ref.is_loaded}")

        # Location objects are already fully instantiated from the Person GET response
        # No additional API requests are needed
        if person_ref.home_location:
            location = person_ref.home_location
            print(f"  Home: {location.formatted_address}")
            if location.coordinate:
                print(
                    f"  Coordinates: {location.coordinate.latitude}, {location.coordinate.longitude}"
                )
            print(f"  Location type: {type(location).__name__}"
                 )  # Should be Location

    # Direct access through get() also works
    if len(persons) > 2:
        print("\nLoading person 3 directly:")
        person = await persons[2].aget()
        print(f"  Name: {person.given_name} {person.family_name}")

    # You can also get a person directly by ID (no lazy loading)
    print("\nGetting a person directly by ID:")
    if persons:
        # Get the first person's ID
        person_id = persons[0].entity_id
        # Fetch directly - this will include fully resolved locations
        direct_person = await fb.entities.persons.aget(person_id)
        print(f"  Name: {direct_person.given_name} {direct_person.family_name}")

        # Check locations - they should be fully resolved Location objects
        print("  Locations:")
        for loc_key, location in direct_person.locations.items():
            print(f"    {loc_key}: {location.formatted_address}")
            print(f"    {loc_key} type: {type(location).__name__}"
                 )  # Should be Location


if __name__ == "__main__":
    asyncio.run(main())
