"""
Working with Event entities in the Fusionbase SDK.

This example demonstrates how to fetch and use event data.
"""

import os

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import ResourceNotFoundError

# Configure logging
configure_logging(level="INFO")

# Event example IDs
SAMPLE_EVENT_ID = "82a68ab9f7151fa0af9bf189c1caa753"


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def fetch_event(client, event_id, name=None):
    """Fetch and display information about an event.

    Args:
        client: Fusionbase client
        event_id: ID of the event to fetch
        name: Optional name for display purposes

    Returns:
        The event entity or None if not found
    """
    try:
        event = client.entities.events.from_id(event_id)

        display_name = name or event.event_title or f"Event {event.fb_entity_id}"
        print(f"Found event: {display_name}")

        # Basic information
        print(f"  Entity ID: {event.fb_entity_id}")
        print(f"  Title: {event.event_title}")
        if event.event_description:
            print(f"  Description: {event.event_description}")
        print(f"  Entity Subtype: {event.entity_subtype}")
        print(f"  Status: {event.status}")
        print(f"  Category: {event.category}")

        # Dates
        if event.start_date:
            print(f"  Start Date: {event.start_date}")
        if event.end_date:
            print(f"  End Date: {event.end_date}")
        if event.announce_date:
            print(f"  Announcement Date: {event.announce_date}")
        if event.live_date:
            print(f"  Live Date: {event.live_date}")

        return event

    except ResourceNotFoundError:
        print(f"Event with ID '{event_id}' not found.")
        return None
    except FusionbaseError as e:
        print(f"Error fetching event: {e}")
        return None


def explore_event_details(event):
    """Explore the detailed properties of an event.

    Args:
        event: An Event entity
    """
    if not event:
        return

    print_separator(f"Exploring {event.event_title or 'Event'}")

    # Source information
    if event.source:
        print("\nSource Information:")
        print(f"  Source ID: {event.source.id or 'Not available'}")

    # Locations
    if event.origin_location:
        print("\nOrigin Location:")
        print(f"  {event.origin_location.formatted_address}")

    if event.event_location:
        print("\nEvent Location:")
        print(f"  {event.event_location.formatted_address}")

    if event.effect_location:
        print("\nEffect Location:")
        print(f"  {event.effect_location.formatted_address}")

    # Linked entities
    if event.linked_person:
        print("\nLinked Person:")
        person = event.linked_person
        print(f"  ID: {person.fb_entity_id}")
        print(f"  Name: {person.given_name} {person.family_name}")

    if event.linked_organization:
        print("\nLinked Organization:")
        org = event.linked_organization
        print(f"  ID: {org.fb_entity_id}")
        print(f"  Name: {org.name}")


def main():
    """Run the events example."""
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
            print_separator("Fetching Events")

            # Fetch a sample event
            sample_event = fetch_event(client, SAMPLE_EVENT_ID, "Sample Event")

            # Explore the event in detail
            if sample_event:
                explore_event_details(sample_event)

            # Try to fetch a non-existent event
            print("\nAttempting to fetch a non-existent event:")
            fetch_event(client, "non_existent_event_id")

            print_separator("Events Example Complete")

        finally:
            # Always close the client to ensure resources are released
            client.close()

    except FusionbaseError as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
