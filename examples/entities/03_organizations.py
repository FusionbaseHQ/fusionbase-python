"""
Working with Organization entities in the Fusionbase SDK.

This example demonstrates how to fetch and use organization data.
"""

import os

from fusionbase import Fusionbase
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.logging import configure_logging

# Configure logging
configure_logging(level="INFO")

# Organization example IDs
ORORATECH_ID = "82a68ab9f7151fa0af9bf189c1caa753"  # OroraTech GmbH


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def fetch_organization(client, organization_id, name=None):
    """Fetch and display information about an organization.

    Args:
        client: Fusionbase client
        organization_id: ID of the organization to fetch
        name: Optional name for display purposes

    Returns:
        The organization entity or None if not found
    """
    try:
        organization = client.entities.organizations.from_id(organization_id)

        display_name = name or organization.name
        print(f"Found organization: {display_name}")

        # Basic information
        print(f"  Entity ID: {organization.fb_entity_id}")
        print(f"  Name: {organization.name}")
        print(f"  Status: {organization.status.status}")
        print(f"  Active: {organization.is_active}")
        print(f"  Entity Subtype: {organization.entity_subtype}")

        # Contact information (if available)
        if organization.primary_website:
            print(f"  Website: {organization.primary_website}")
        if organization.primary_phone:
            print(f"  Phone: {organization.primary_phone}")

        # Address (if available)
        if organization.address:
            print(f"  Address: {organization.address.formatted_address}")

        return organization

    except ResourceNotFoundError:
        print(f"Organization with ID '{organization_id}' not found.")
        return None
    except FusionbaseError as e:
        print(f"Error fetching organization: {e}")
        return None


def explore_organization_details(organization):
    """Explore the detailed properties of an organization.

    Args:
        organization: An Organization entity
    """
    if not organization:
        return

    print_separator(f"Exploring {organization.name}")

    # Description
    if organization.description and 'registry' in organization.description:
        print("Description:")
        for lang, desc in organization.description["registry"].items():
            print(f"  {lang}: {desc}")

    # Alternative names
    if organization.other_names:
        print("\nAlternative Names:")
        for alt_name in organization.other_names:
            print(f"  {alt_name}")

    # Address details
    if organization.address:
        print("\nAddress Details:")
        print(f"  Formatted Address: {organization.address.formatted_address}")

        if organization.address.coordinate:
            print(f"  Coordinates: {organization.address.coordinate.latitude}, "
                  f"{organization.address.coordinate.longitude}")

        # Address components
        if organization.address.address_components:
            print("\n  Address Components:")
            for component in organization.address.address_components:
                print(
                    f"    {component.component_type}: {component.component_value}"
                )

    # Jurisdiction
    if organization.jurisdiction:
        print("\nJurisdiction:")
        print(f"  Country Code: {organization.jurisdiction.iso_alpha3}")

    # Contact information
    if organization.contact:
        print("\nContact Information:")
        if organization.contact.websites and organization.contact.websites.primary:
            print(f"  Website: {organization.contact.websites.primary}")
        if organization.contact.phone_numbers and organization.contact.phone_numbers.primary:
            print(f"  Phone: {organization.contact.phone_numbers.primary}")
        if organization.contact.emails and organization.contact.emails.primary:
            print(f"  Email: {organization.contact.emails.primary}")

    # Founding and cessation dates
    if organization.founding_date:
        if isinstance(organization.founding_date, str):
            print(f"\nFounded: {organization.founding_date}")
        else:
            print(
                f"\nFounded: {organization.founding_date.strftime('%Y-%m-%d')}")

    if organization.cessation_date:
        if isinstance(organization.cessation_date, str):
            print(f"Ceased: {organization.cessation_date}")
        else:
            print(f"Ceased: {organization.cessation_date.strftime('%Y-%m-%d')}")

    # Classifications
    if organization.classifications and organization.classifications.web:
        print("\nClassifications:")
        for classification in organization.classifications.web:
            print(f"  {classification.source}: {classification.value.de}")

    # Legal information
    if organization.legal and organization.legal.registration_authority:
        print("\nRegistration Information:")
        for auth_key, auth in organization.legal.registration_authority.items():
            print(f"  Registration Authority: {auth_key}")
            if auth.registration_authority_name:
                print(f"  Authority Name: {auth.registration_authority_name}")
            if auth.registration_type and auth.registration_number:
                print(
                    f"  Registration: {auth.registration_type} {auth.registration_number}"
                )
            if auth.registration_authority_location:
                print(
                    f"  Location: {auth.registration_authority_location.formatted_address}"
                )


def main():
    """Run the organizations example."""
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
            print_separator("Fetching Organizations")

            # Fetch a sample organization
            sample_org = fetch_organization(client, ORORATECH_ID,
                                            "OroraTech GmbH")

            # Explore the organization in detail
            if sample_org:
                explore_organization_details(sample_org)

            # Try to fetch a non-existent organization
            print("\nAttempting to fetch a non-existent organization:")
            fetch_organization(client, "non_existent_organization_id")

            print_separator("Organizations Example Complete")

        finally:
            # Always close the client to ensure resources are released
            client.close()

    except FusionbaseError as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
