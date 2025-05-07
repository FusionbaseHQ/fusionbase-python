"""
Searching data within DataStreams.

This example demonstrates how to search for data within a stream
using the search_data method.
"""

import os
from pprint import pprint

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging

# Configure logging for better debugging
configure_logging(level="INFO")

# Example stream ID - replace with an actual stream ID from your account
# This example uses a stream from the German company register
SAMPLE_STREAM_ID = "23532363"  # Replace with your stream ID


def print_separator(title):
    """Print a section separator."""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def basic_search_example(stream):
    """Demonstrate basic text search within a stream."""
    print_separator("Basic Text Search")

    # Get a sample of data to find something to search for
    sample_data = stream.get_data(limit=1)

    if not sample_data:
        print("No data available to search")
        return

    print("Sample record:")
    pprint(sample_data[0])

    # Find a string field to search for
    search_term = None
    for key, value in sample_data[0].items():
        if isinstance(value, str) and len(value) > 3:
            search_term = value[:3]  # Take first 3 chars
            print(
                f"\nSearching for term: '{search_term}' (first 3 chars of field '{key}')"
            )
            break

    if not search_term:
        print("No suitable search term found")
        return

    # Search for data containing that term
    results = stream.search_data(q=search_term, limit=5)

    print(f"\nFound {len(results)} matching records")

    # Display the first few results
    for i, record in enumerate(results[:3], 1):
        print(f"\nResult {i}:")
        for key, value in record.items():
            # Truncate long values for display
            display_value = value
            if isinstance(value, str) and len(value) > 50:
                display_value = value[:47] + "..."
            print(f"  {key}: {display_value}")


def pagination_example(stream):
    """Demonstrate paginating through search results."""
    print_separator("Search Pagination")

    # Choose a common search term that likely returns multiple results
    search_term = "A"  # This should match many strings

    # First page - limit to 3 results
    page1 = stream.search_data(q=search_term, skip=0, limit=3)
    print(f"Page 1 results: {len(page1)} items")

    if page1:
        # Display first result ID or a key field
        first_key = next(iter(page1[0].keys()))  # Get the first key
        print(f"First result's {first_key}: {page1[0][first_key]}")

    # Second page
    page2 = stream.search_data(q=search_term, skip=3, limit=3)
    print(f"Page 2 results: {len(page2)} items")

    if page1 and page2:
        # Verify the pages contain different records
        first_key = next(iter(page1[0].keys()))  # Get the first key
        if first_key in page2[0]:
            print(
                f"Pages contain different records: {page1[0][first_key] != page2[0][first_key]}"
            )


def dataframe_example(stream):
    """Demonstrate returning search results as pandas DataFrame."""
    print_separator("Search Results as pandas DataFrame")

    try:
        import pandas as pd

        # Choose a search term
        search_term = "A"  # This should match many strings

        # Get results as DataFrame
        df = stream.search_data(q=search_term, limit=5, return_type="dataframe")

        print(f"DataFrame shape: {df.shape}")
        print("\nColumn names:")
        print(df.columns.tolist())

        print("\nFirst 3 rows:")
        print(df.head(3))

    except ImportError:
        print("pandas is not installed. Install with 'pip install pandas'")


def main():
    """Run the data search examples."""
    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        return

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:
        # Get a DataStream from ID
        stream = client.streams.from_id(SAMPLE_STREAM_ID)

        # Get some basic info about the stream
        metadata = stream.get_metadata()
        print(f"Stream: {metadata.display_name}")
        print(f"Total records: {metadata.meta.entry_count}")

        # Run examples
        basic_search_example(stream)
        pagination_example(stream)
        dataframe_example(stream)

    except Exception as e:
        import traceback
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()
