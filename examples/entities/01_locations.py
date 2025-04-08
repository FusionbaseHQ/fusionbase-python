"""
Working with Location entities in the Fusionbase SDK.

This example demonstrates how to fetch and use location data.
"""

import os

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import ResourceNotFoundError

# Configure logging
configure_logging(level="INFO")

# Location example IDs
MUNICH_ID = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich, Germany


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def fetch_location(client, location_id, name=None):
    """Fetch and display information about a location.

    Args:
        client: Fusionbase client
        location_id: ID of the location to fetch
        name: Optional name for display purposes

    Returns:
        The location entity or None if not found
    """
    try:
        location = client.entities.locations.from_id(location_id)

        display_name = name or location.formatted_address
        print(f"Found location: {display_name}")

        # Basic information
        print(f"  Entity ID: {location.fb_entity_id}")
        print(f"  Formatted Address: {location.formatted_address}")
        print(f"  Entity Subtype: {location.entity_subtype}")

        # Coordinates (if available)
        if location.coordinate:
            # pylint: disable=line-too-long
            print(
                f"  Coordinates: {location.coordinate.latitude}, {location.coordinate.longitude}"
            )

        return location

    except ResourceNotFoundError:
        print(f"Location with ID '{location_id}' not found.")
        return None
    except FusionbaseError as e:
        print(f"Error fetching location: {e}")
        return None


def explore_location_details(location):
    """Explore the detailed properties of a location.

    Args:
        location: A Location entity
    """
    if not location:
        return

    print_separator(f"Exploring {location.formatted_address}")

    # Address components
    print("Address Components:")
    for component in location.address_components:
        print(f"  {component.component_type}: {component.component_value}")

    # Convenience properties
    print("\nConvenience Properties:")
    print(f"  City: {location.city or 'N/A'}")
    print(f"  State/Region: {location.state or 'N/A'}")
    print(f"  Country: {location.country or 'N/A'}")
    print(f"  Postal Code: {location.postal_code or 'N/A'}")
    print(f"  Street: {location.street or 'N/A'}")
    print(f"  House Number: {location.house_number or 'N/A'}")

    # Alternative names (if available)
    if location.alternative_names:
        print("\nAlternative Names:")
        for alt_name in location.alternative_names:
            print(f"  {alt_name}")


def main():
    """Run the locations example."""
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
            print_separator("Fetching Locations")

            # Fetch Munich
            munich = fetch_location(client, MUNICH_ID, "Munich")

            # Explore Munich in detail
            if munich:
                explore_location_details(munich)

            # Try to fetch a non-existent location
            print("\nAttempting to fetch a non-existent location:")
            fetch_location(client, "non_existent_location_id")

            print_separator("Locations Example Complete")

        finally:
            # Always close the client to ensure resources are released
            client.close()

    except FusionbaseError as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
