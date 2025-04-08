"""
Working with Person entities in the Fusionbase SDK.

This example demonstrates how to fetch and use person data.
"""

import os

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import ResourceNotFoundError

# Configure logging
configure_logging(level="INFO")

# Person example IDs
SAMPLE_PERSON_ID = "e6ce61d930d72a0659c066fa37ca42c7"  # Patrick Holl


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def fetch_person(client, person_id, name=None):
    """Fetch and display information about a person.

    Args:
        client: Fusionbase client
        person_id: ID of the person to fetch
        name: Optional name for display purposes

    Returns:
        The person entity or None if not found
    """
    try:
        person = client.entities.persons.from_id(person_id)

        display_name = name or f"{person.given_name} {person.family_name}"
        print(f"Found person: {display_name}")

        # Basic information
        print(f"  Entity ID: {person.fb_entity_id}")
        print(f"  Name: {person.given_name} {person.family_name}")
        print(f"  Entity Subtype: {person.entity_subtype}")

        # Birth date (if available)
        if person.birth_date and person.birth_date.value:
            print(
                f"  Birth Date: {person.birth_date.value.strftime('%Y-%m-%d')}")

        return person

    except ResourceNotFoundError:
        print(f"Person with ID '{person_id}' not found.")
        return None
    except FusionbaseError as e:
        print(f"Error fetching person: {e}")
        return None


def explore_person_details(person):
    """Explore the detailed properties of a person.

    Args:
        person: A Person entity
    """
    if not person:
        return

    print_separator(f"Exploring {person.given_name} {person.family_name}")

    # Name components
    print("Name Components:")
    print(f"  Given Name: {person.name.given}")
    print(f"  Family Name: {person.name.family}")
    if person.name.maiden:
        print(f"  Maiden Name: {person.name.maiden}")
    if person.name.aliases:
        print("\nAliases:")
        for alias in person.name.aliases:
            print(f"  {alias}")

    # Home location (if available)
    if person.home_location:
        print("\nHome Location:")
        print(f"  Address: {person.home_location.formatted_address}")
        if person.home_location.coordinate:
            print(f"  Coordinates: {person.home_location.coordinate.latitude}, "
                  f"{person.home_location.coordinate.longitude}")

        # Location address components
        if person.home_location.address_components:
            print("\n  Address Components:")
            for component in person.home_location.address_components:
                print(
                    f"    {component.component_type}: {component.component_value}"
                )

    # Source information (if available)
    if person.source and person.source.id:
        print("\nSource Information:")
        print(f"  Source ID: {person.source.id}")


def main():
    """Run the persons example."""
    # Check for API key
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("Error: FUSIONBASE_API_KEY environment variable not set.")
        print(
            "Please set your API key with: export FUSIONBASE_API_KEY=your_api_key_here"
        )
        return

    try:
        # Create client with proper resource management
        client = Fusionbase(api_key=api_key)

        try:
            print_separator("Fetching Persons")

            # Fetch a sample person
            sample_person = fetch_person(client, SAMPLE_PERSON_ID,
                                         "Sample Person")

            # Explore the person in detail
            if sample_person:
                explore_person_details(sample_person)

            # Try to fetch a non-existent person
            print("\nAttempting to fetch a non-existent person:")
            fetch_person(client, "non_existent_person_id")

            print_separator("Persons Example Complete")

        finally:
            # Always close the client to ensure resources are released
            client.close()

    except FusionbaseError as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
