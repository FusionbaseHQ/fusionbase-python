"""
Error handling with the Fusionbase SDK.

This example demonstrates how to handle various errors that might occur.
"""

from fusionbase import Fusionbase
from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import AuthorizationError
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import RequestValidationError as ValidationError
from fusionbase.exceptions import ResourceNotFoundError


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def demonstrate_resource_not_found(client):
    """Demonstrate handling of ResourceNotFoundError."""
    print_separator("Handling ResourceNotFoundError")

    try:
        # Try to fetch a non-existent location
        print("Attempting to fetch a non-existent location...")
        client.entities.locations.from_id("non_existing_location_id")
        print("This should not be reached!")

    except ResourceNotFoundError as e:
        print("✓ ResourceNotFoundError caught as expected!")
        print(f"Status code: {e.status_code}")
        print(f"Resource type: {e.resource_type}")
        print(f"Resource ID: {e.resource_id}")
        print(f"Error message: {str(e)}")


def demonstrate_try_except_pattern(client):
    """Demonstrate the recommended try-except pattern."""
    print_separator("Recommended Try-Except Pattern")

    try:
        # Try to fetch a valid location
        print("Attempting to fetch a valid location...")
        munich = client.entities.locations.from_id(
            "bfcc19ddd9edb12efb9cfea181b0dcd3")
        print(f"✓ Success! Found: {munich.formatted_address}")

    except ResourceNotFoundError as e:
        print(f"Location not found: {e}")

    except AuthenticationError as e:
        print(f"Authentication failed: {e}")

    except AuthorizationError as e:
        print(f"Authorization failed: {e}")

    except ValidationError as e:
        print(f"Validation failed: {e}")

    except APIError as e:
        print(f"API error: {e}")

    except FusionbaseError as e:
        # Catch-all for any Fusionbase-specific errors
        print(f"Fusionbase error: {e}")

    except Exception as e:  # pylint: disable=broad-exception-caught
        # Catch any other unexpected errors
        print(f"Unexpected error: {e}")


def main():
    """Run the error handling examples."""
    try:
        with Fusionbase() as client:
            # Demonstrate different error handling scenarios
            demonstrate_resource_not_found(client)
            demonstrate_try_except_pattern(client)

            print_separator("Error Handling Complete")

    except ValueError as e:
        print(f"Error initializing client: {e}")
        print("Make sure to set the FUSIONBASE_API_KEY environment variable.")


if __name__ == "__main__":
    main()
