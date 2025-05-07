"""
Basic usage of DataStream in the Fusionbase SDK.

This example demonstrates how to initialize and use DataStream
for basic data retrieval operations.
"""

import os
from pprint import pprint

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging

# Configure logging for better debugging
configure_logging(level="INFO")

# Example stream ID - replace with an actual stream ID from your account
SAMPLE_STREAM_ID = "23532363"  # Replace with your stream ID


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def basic_initialization_example(client):
    """Demonstrate different ways to initialize a DataStream instance."""
    print_separator("DataStream Initialization")

    # Method 1: From ID using streams manager
    print("1. Initialize using streams manager with ID:")
    stream = client.streams.from_id(SAMPLE_STREAM_ID)
    print(f"  ✓ Stream initialized with ID: {stream.stream_key}")

    # Method 2: From ID with full prefix
    print("\n2. Initialize with full ID including collection prefix:")
    full_id = f"data_streams/{SAMPLE_STREAM_ID}"
    stream = client.streams.from_id(full_id)
    print(f"  ✓ Stream initialized with full ID: {stream.stream_id}")
    print(f"  ✓ Stream key extracted: {stream.stream_key}")

    return stream


def show_metadata_example(stream):
    """Demonstrate retrieving stream metadata."""
    print_separator("Stream Metadata")

    # Get metadata
    metadata = stream.get_metadata()

    # Display key metadata properties
    print(f"Stream name: {metadata.display_name}")
    print(f"Entry count: {metadata.meta.entry_count}")
    print(f"Number of columns: {metadata.meta.main_property_count}")
    print(f"Is active: {metadata.meta.is_active}")

    # Display stream columns/properties
    print("\nStream columns:")
    for i, column in enumerate(metadata.data_item_collections, 1):
        print(
            f"  {i}. {column.name} ({column.basic_data_type or 'unknown type'})"
        )
        if column.description and column.description.en:
            print(f"     Description: {column.description.en}")

    # Display data version info
    print(f"\nCurrent version: {metadata.data_version}")
    print(f"Last updated: {metadata.data_updated_at}")


def retrieve_data_example(stream):
    """Demonstrate retrieving data from the stream."""
    print_separator("Retrieving Data")

    # Get first 5 records with default format
    print("Getting first 5 records:")
    data = stream.get_data(limit=5)

    # Print the records with nice formatting
    for i, record in enumerate(data, 1):
        print(f"\nRecord {i}:")
        pprint(record, indent=2, depth=1, width=100)

    # Get total count of records
    metadata = stream.get_metadata()
    print(f"\nTotal records in stream: {metadata.meta.entry_count}")


def main():
    """Run the basic DataStream examples."""
    # Check for API key
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("Error: FUSIONBASE_API_KEY environment variable not set.")
        print(
            "Please set your API key with: export FUSIONBASE_API_KEY=your_api_key_here"
        )
        return

    try:
        # Create client using context manager for proper resource handling
        with Fusionbase(api_key=api_key) as client:
            print_separator("DataStream Basic Usage Examples")

            # Check if the sample stream ID has been set
            if SAMPLE_STREAM_ID == "12345":
                print(
                    "⚠️  Please replace SAMPLE_STREAM_ID with an actual stream ID!"
                )
                print("Attempting to continue with example ID, which may fail.")

            # Run basic examples
            try:
                stream = basic_initialization_example(client)
                show_metadata_example(stream)
                retrieve_data_example(stream)

                print_separator("Basic Usage Examples Complete")

            except Exception as e:
                print(f"Error in example: {e}")
                print(
                    "If this is due to an invalid stream ID, please update the SAMPLE_STREAM_ID."
                )

    except Exception as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
