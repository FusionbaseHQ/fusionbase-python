"""
Asynchronous searching within DataStreams.

This example demonstrates how to asynchronously search for data
within a stream using the asearch_data method.
"""

import asyncio
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
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


async def basic_search_example(stream):
    """Demonstrate basic async text search within a stream."""
    print_separator("Basic Async Text Search")

    # Get a sample of data to find something to search for
    sample_data = await stream.aget_data(limit=1)

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
    results = await stream.asearch_data(q=search_term, limit=5)

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


async def concurrent_searches_example(stream):
    """Demonstrate running multiple searches concurrently."""
    print_separator("Concurrent Searches")

    # Define several search terms
    search_terms = ["A", "B", "C", "1", "2"]

    # Create tasks for all searches
    search_tasks = [
        stream.asearch_data(q=term, limit=3) for term in search_terms
    ]

    # Run all searches concurrently
    results = await asyncio.gather(*search_tasks)

    # Display summary of results
    for term, matches in zip(search_terms, results):
        print(f"Search for '{term}' found {len(matches)} matches")


async def dataframe_example(stream):
    """Demonstrate returning search results as pandas DataFrame."""
    print_separator("Async Search Results as pandas DataFrame")

    try:
        import pandas as pd

        # Choose a search term
        search_term = "A"  # This should match many strings

        # Get results as DataFrame
        df = await stream.asearch_data(q=search_term,
                                       limit=5,
                                       return_type="dataframe")

        print(f"DataFrame shape: {df.shape}")
        print("\nColumn names:")
        print(df.columns.tolist())

        print("\nFirst few rows:")
        print(df.head(3))

    except ImportError:
        print("pandas is not installed. Install with 'pip install pandas'")


async def run_examples():
    """Run all examples asynchronously."""
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
        metadata = await stream.aget_metadata()
        print(f"Stream: {metadata.display_name}")
        print(f"Total records: {metadata.meta.entry_count}")

        # Run examples
        await basic_search_example(stream)
        await concurrent_searches_example(stream)
        await dataframe_example(stream)

    except Exception as e:
        import traceback
        print(f"ERROR: {e}")
        traceback.print_exc()
    finally:
        # Always close the client
        await client.aclose()
        client.close()


def main():
    """Entry point for the example."""
    asyncio.run(run_examples())


if __name__ == "__main__":
    main()
