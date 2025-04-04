"""
Getting started with the Fusionbase SDK.

This example demonstrates how to initialize the client and perform basic operations.
"""
from fusionbase import Fusionbase
from fusionbase.logging import configure_logging

# Configure basic logging
configure_logging(level="INFO")


def main():
    """Run the basic client initialization example."""
    print("=== Getting Started with Fusionbase SDK ===\n")

    # Method 1: Initialize with API key from environment variable
    print("1. Initializing client with API key from environment variable...")
    try:
        client = Fusionbase()
        print("✓ Client initialized successfully!")
    except ValueError as e:
        print(f"✗ Error: {e}")
        print("  Make sure to set the FUSIONBASE_API_KEY environment variable.")
        client = None

    # If method 1 failed and you want to try with explicit API key
    if client is None:
        print("\n2. Initializing client with explicit API key...")
        api_key = input("Enter your Fusionbase API key: ")
        try:
            client = Fusionbase(api_key=api_key)
            print("✓ Client initialized successfully!")
        except ValueError as e:
            print(f"✗ Error: {e}")
            return

    print("\n3. Using the client with context manager (recommended)...")
    with client:
        print(f"✓ Connected to API at: {client.base_url}")

    print("\n=== Getting Started Complete ===")


if __name__ == "__main__":
    main()
