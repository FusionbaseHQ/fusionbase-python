"""
Asynchronous operations with the Fusionbase SDK.

This example demonstrates how to use the SDK's async capabilities.
"""

import asyncio
import time

from fusionbase import Fusionbase
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.logging import configure_logging

# Configure logging
configure_logging(level="INFO")


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


async def fetch_location_async(client, location_id, label=None):
    """Fetch a location asynchronously.

    Args:
        client: Fusionbase async client
        location_id: ID of the location to fetch
        label: Optional label for logging

    Returns:
        Location entity or None if not found
    """
    try:
        start = time.time()
        location = await client.entities.locations.afrom_id(location_id)
        duration = time.time() - start

        if label:
            print(
                f"{label}: {location.formatted_address} fetched in {duration:.4f}s"
            )
        else:
            print(
                f"Location {location.formatted_address} fetched in {duration:.4f}s"
            )

        return location
    except ResourceNotFoundError:
        print(f"Location ID {location_id} not found")
        return None
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error fetching location {location_id}: {e}")
        return None


async def demonstrate_concurrent_requests(client):
    """Demonstrate making concurrent requests."""
    print_separator("Concurrent Requests")

    # We'll fetch the same location multiple times to demonstrate concurrency
    # In a real application, you'd likely fetch different resources
    location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich

    print("Fetching location 3 times concurrently...")
    start_time = time.time()

    results = await asyncio.gather(
        fetch_location_async(client, location_id, "Request 1"),
        fetch_location_async(client, location_id, "Request 2"),
        fetch_location_async(client, location_id, "Request 3"))

    total_time = time.time() - start_time
    print(f"All requests completed in {total_time:.4f}s")

    # Count successful results
    successful = sum(1 for result in results if result is not None)
    print(f"{successful} of {len(results)} requests were successful")


async def demonstrate_sequential_vs_parallel(client):
    """Compare sequential vs parallel requests."""
    print_separator("Sequential vs Parallel Requests")

    # Create a list of location IDs (we'll use the same one multiple times)
    location_id = "bfcc19ddd9edb12efb9cfea181b0dcd3"  # Munich
    location_ids = [location_id] * 3

    # Sequential requests
    print("Running 3 sequential requests...")
    start = time.time()

    for i, loc_id in enumerate(location_ids):
        await fetch_location_async(client, loc_id, f"Sequential {i+1}")

    sequential_time = time.time() - start
    print(f"Sequential requests completed in {sequential_time:.4f}s")

    # Parallel requests
    print("\nRunning 3 parallel requests...")
    start = time.time()

    tasks = [
        fetch_location_async(client, loc_id, f"Parallel {i+1}")
        for i, loc_id in enumerate(location_ids)
    ]
    await asyncio.gather(*tasks)

    parallel_time = time.time() - start
    print(f"Parallel requests completed in {parallel_time:.4f}s")

    # Compare performance
    if sequential_time > parallel_time:
        speedup = sequential_time / parallel_time
        print(
            f"\n✓ Parallel requests were {speedup:.1f}x faster than sequential!"
        )
    else:
        print(
            "\n⚠️ Parallel requests weren't faster (caching may be affecting results)"
        )


async def run_async_demos():
    """Run all async demonstrations."""
    try:
        # Create a regular client first
        client = Fusionbase()

        try:
            # Run the async demonstrations
            await demonstrate_concurrent_requests(client.async_client)
            await demonstrate_sequential_vs_parallel(client.async_client)

            print_separator("Async Examples Complete")

        finally:
            # Make sure to close the client
            client.close()

    except ValueError as e:
        print(f"Error initializing client: {e}")
        print("Make sure to set the FUSIONBASE_API_KEY environment variable.")


def main():
    """Entry point for the async examples."""
    asyncio.run(run_async_demos())


if __name__ == "__main__":
    main()
