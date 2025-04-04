"""Example demonstrating search with lazy loading of entities."""

import asyncio
import os
from typing import List

from fusionbase import Fusionbase
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.location import Location
from fusionbase.entities.person import Person


async def search_persons(fb: Fusionbase):
    """Search for persons with lazy loading."""
    print("--- Person Search Example ---")

    # Search with parameters directly in the method call
    # Use consistent parameter names: q, skip, limit
    results = await fb.search.persons.asearch(q="John", skip=0, limit=3)
    print(f"Found {results.total} persons")
    print(
        f"Results {results.skip+1}-{min(results.skip+len(results.items), results.total)} of {results.total}"
    )

    persons: List[LazyReference[Person]] = results.items

    # Accessing a property will trigger loading of the entity
    if persons:
        person_ref = persons[0]
        print(f"\nPerson: {person_ref.given_name} {person_ref.family_name}")

        # After first access, the entity is fully loaded
        print(
            f"Home location: {person_ref.home_location.formatted_address if person_ref.home_location else 'None'}"
        )
        print(f"Is loaded? {person_ref.is_loaded}")


async def search_locations(fb: Fusionbase):
    """Search for locations with lazy loading."""
    print("\n--- Location Search Example ---")

    # Search for a location with consistent parameter naming
    results = await fb.search.locations.asearch(q="Munich", skip=0, limit=5)
    print(f"Found {results.total} locations")
    print(
        f"Results {results.skip+1}-{min(results.skip+len(results.items), results.total)} of {results.total}"
    )

    locations: List[LazyReference[Location]] = results.items

    # Load a location explicitly
    if locations:
        location = await locations[0].aget()
        print(f"\nLocation: {location.formatted_address}")
        print(
            f"Coordinates: {location.coordinate.latitude}, {location.coordinate.longitude}"
            if location.coordinate else "No coordinates")


async def main():
    """Run the example."""
    # Initialize Fusionbase client
    fb = Fusionbase(api_key=os.environ.get("FUSIONBASE_API_KEY"))

    await search_persons(fb)
    await search_locations(fb)


if __name__ == "__main__":
    asyncio.run(main())
