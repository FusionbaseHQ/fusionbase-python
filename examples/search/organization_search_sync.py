"""
Example demonstrating synchronous organization search with the Fusionbase SDK.

This example shows how to search for organizations and handle the search results.
"""

import os
import sys
import traceback

from fusionbase import Fusionbase
from fusionbase.search.organization_search import OrganizationSearchParams
from fusionbase.types.entities import FilterKey


def safely_get_organization(org_ref):
    """Safely retrieve an organization from a reference with error handling."""
    try:
        return org_ref.get()
    except Exception as e:
        print(f"  Error loading organization: {e}")
        return None


def display_organization_details(org):
    """Display details about an organization."""
    if not org:
        return False

    print("\nOrganization details:")
    print(f"  ID: {org.fb_entity_id}")
    print(f"  Name: {org.name}")

    # Display status
    if org.status:
        print(f"  Status: {org.status.status}")
        print(f"  Active: {org.is_active}")

    # Display address if available
    if org.address:
        print(f"  Address: {org.address.formatted_address}")

    # Display contact information if available
    if org.primary_website:
        print(f"  Website: {org.primary_website}")
    if org.primary_phone:
        print(f"  Phone: {org.primary_phone}")

    # Display jurisdiction if available
    if org.jurisdiction:
        print(f"  Jurisdiction: {org.jurisdiction.iso_alpha3}")

    return True


def main():
    """Run synchronous organization search examples."""
    print("=== Synchronous Organization Search Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Example 1: Basic organization search
        print("1. Basic organization search for 'OroraTech'...")
        params = OrganizationSearchParams(q="OroraTech")
        results = client.search.organizations.search(params)
        print(f"Found {len(results.items)} results")

        # Display the first result
        if results.items:
            org_ref = results.items[0]
            # Get the organization from LazyReference with error handling
            org = safely_get_organization(org_ref)
            if not display_organization_details(org):
                print("Could not display organization details")
        else:
            print("No results found")

        # Example 2: Search with postal code
        print("\n2. Search for organizations in a specific postal code area...")
        # Use FilterKey enum for filter keys
        params = OrganizationSearchParams(
            q="GmbH", filters={FilterKey.POSTAL_CODE: "80992"})
        results = client.search.organizations.search(params)
        print(f"Found {len(results.items)} results in postal code 80992")

        # Example 3: Searching with organization subtype
        print("\n3. Searching for corporations...")
        # Currently this is done through a generic filter in the query
        # We include it in the query string
        params = OrganizationSearchParams(q="corporation")
        results = client.search.organizations.search(params)
        print(f"Found {len(results.items)} corporation results")

        # Example 4: Pagination example
        print("\n4. Pagination example...")
        first_page = client.search.organizations.search(q="GmbH",
                                                        limit=5,
                                                        skip=0)
        second_page = client.search.organizations.search(q="GmbH",
                                                         limit=5,
                                                         skip=5)

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
