"""
Chunking and iteration with DataStream.

This example demonstrates processing large datasets efficiently
using chunks and stream iteration features.
"""

import asyncio
import os
import time

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.data.datastream import ChunkingStrategy

# Configure logging for better debugging
configure_logging(level="INFO")

# Example stream ID - replace with an actual stream ID from your account
SAMPLE_STREAM_ID = "23532363"  # Replace with your stream ID


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def basic_chunk_iteration(stream):
    """Demonstrate basic chunk iteration for processing large datasets."""
    print_separator("Basic Chunk Iteration")

    # Process data in chunks using iter_chunks
    print("Processing data in chunks using iter_chunks:")

    # Set up counters to track progress
    total_records = 0
    chunk_count = 0

    # Process each chunk
    start_time = time.time()

    for chunk in stream.iter_chunks(chunk_size=100):
        chunk_count += 1
        chunk_size = len(chunk)
        total_records += chunk_size

        print(f"Processing chunk {chunk_count}: {chunk_size} records")

        # Example: Calculate average value of first numeric field (if any)
        sample_record = chunk[0] if chunk else {}
        numeric_field = None

        # Find first numeric field in the record
        for key, value in sample_record.items():
            if isinstance(value, (int, float)):
                numeric_field = key
                break

        if numeric_field:
            # Calculate average of numeric field
            values = [record.get(numeric_field, 0) for record in chunk]
            if values:
                avg = sum(values) / len(values)
                print(f"  Average of {numeric_field}: {avg:.2f}")

        # Stop after 3 chunks for the example
        if chunk_count >= 3:
            print("Stopping after 3 chunks for demonstration purposes.")
            break

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\nProcessed {total_records} records in {chunk_count} chunks")
    print(f"Total processing time: {elapsed:.2f} seconds")
    print(
        f"Average processing time per chunk: {elapsed/chunk_count:.2f} seconds")
    print(
        f"Average processing time per record: {elapsed/total_records:.4f} seconds"
    )


def chunking_strategies(stream):
    """Demonstrate different chunking strategies."""
    print_separator("Chunking Strategies")

    # 1. Auto Strategy (default)
    print("1. AUTO strategy (automatically determine chunk size):")
    auto_chunk = next(stream.iter_chunks(strategy=ChunkingStrategy.AUTO))
    print(f"  Auto-determined chunk size: {len(auto_chunk)} records")

    # 2. Fixed Strategy
    print("\n2. FIXED strategy (use a specific chunk size):")
    fixed_chunk = next(
        stream.iter_chunks(strategy=ChunkingStrategy.FIXED, chunk_size=50))
    print(f"  Fixed chunk size: {len(fixed_chunk)} records")

    # 3. Memory Strategy
    print("\n3. MEMORY strategy (adjust based on available memory):")
    memory_chunk = next(
        stream.iter_chunks(
            strategy=ChunkingStrategy.MEMORY,
            max_memory_percent=0.05  # Use only 5% of available memory
        ))
    print(f"  Memory-based chunk size: {len(memory_chunk)} records")


def row_by_row_iteration(stream):
    """Demonstrate row-by-row iteration using the iterable interface."""
    print_separator("Row-by-Row Iteration")

    print("Iterating through stream row by row:")

    # Configure query options for the iteration
    stream.set_query_options(
        chunk_size=100,  # Still fetches in chunks of 100 behind the scenes
        chunking_strategy=ChunkingStrategy.FIXED)

    # Use the stream as an iterator (processes one row at a time)
    counter = 0
    start_time = time.time()

    for record in stream:
        # Process each individual record
        counter += 1

        # Just print the first field for demonstration
        if counter == 1:
            first_key = next(iter(record.keys()))
            print(f"First record, first field: {first_key}={record[first_key]}")

        # Print status every 100 records
        if counter % 100 == 0:
            print(f"Processed {counter} records...")

        # Stop after 300 records for the example
        if counter >= 300:
            print("Stopping after 300 records for demonstration purposes.")
            break

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\nProcessed {counter} records")
    print(f"Total processing time: {elapsed:.2f} seconds")
    print(f"Average time per record: {elapsed/counter:.4f} seconds")


async def async_iteration_example(stream):
    """Demonstrate asynchronous iteration."""
    print_separator("Asynchronous Iteration")

    print("Iterating through stream asynchronously:")

    # Configure query options for the iteration
    stream.set_query_options(
        chunk_size=100,  # Chunk size for async fetching
        chunking_strategy=ChunkingStrategy.AUTO)

    # Use the stream as an async iterator
    counter = 0
    start_time = time.time()

    async for record in stream:
        # Process each individual record asynchronously
        counter += 1

        # Just print the first field for demonstration
        if counter == 1:
            first_key = next(iter(record.keys()))
            print(f"First record, first field: {first_key}={record[first_key]}")

        # Simulate async processing
        if counter % 50 == 0:
            # Simulate some async work
            await asyncio.sleep(0.01)
            print(f"Processed {counter} records...")

        # Stop after 300 records for the example
        if counter >= 300:
            print("Stopping after 300 records for demonstration purposes.")
            break

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\nAsynchronously processed {counter} records")
    print(f"Total processing time: {elapsed:.2f} seconds")
    print(f"Average time per record: {elapsed/counter:.4f} seconds")


async def async_chunk_iteration_example(stream):
    """Demonstrate asynchronous iteration over chunks."""
    print_separator("Asynchronous Chunk Iteration")

    print("Processing data in chunks asynchronously:")

    # Set up counters to track progress
    total_records = 0
    chunk_count = 0

    # Process each chunk asynchronously
    start_time = time.time()

    async for chunk in stream._aiter_chunks_internal(chunk_size=100):
        chunk_count += 1
        chunk_size = len(chunk)
        total_records += chunk_size

        print(f"Processing chunk {chunk_count}: {chunk_size} records")

        # Simulate async processing
        await asyncio.sleep(0.05)

        # Stop after 3 chunks for the example
        if chunk_count >= 3:
            print("Stopping after 3 chunks for demonstration purposes.")
            break

    end_time = time.time()
    elapsed = end_time - start_time

    print(
        f"\nAsynchronously processed {total_records} records in {chunk_count} chunks"
    )
    print(f"Total processing time: {elapsed:.2f} seconds")


async def run_async_examples(stream):
    """Run all async examples."""
    await async_iteration_example(stream)
    await async_chunk_iteration_example(stream)


def main():
    """Run the chunking and iteration examples."""
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
            print_separator("DataStream Chunking and Iteration Examples")

            # Check if the sample stream ID has been set
            if SAMPLE_STREAM_ID == "12345":
                print(
                    "⚠️  Please replace SAMPLE_STREAM_ID with an actual stream ID!"
                )
                print("Attempting to continue with example ID, which may fail.")

            # Initialize stream
            stream = client.streams.from_id(SAMPLE_STREAM_ID)

            # Run synchronous examples
            try:
                basic_chunk_iteration(stream)
                chunking_strategies(stream)
                row_by_row_iteration(stream)

                # Run async examples
                asyncio.run(run_async_examples(stream))

                print_separator("Chunking and Iteration Examples Complete")

            except Exception as e:
                print(f"Error in example: {e}")
                import traceback
                traceback.print_exc()
                print(
                    "If this is due to an invalid stream ID, please update the SAMPLE_STREAM_ID."
                )

    except Exception as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
