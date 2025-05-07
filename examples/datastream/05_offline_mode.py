"""
Working with DataStream in offline mode.

This example demonstrates how to use DataStream in offline mode,
exporting data to files and loading it back without requiring API access.
"""

import os
from pathlib import Path
import tempfile
import time

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.data.datastream import DataStream

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Pandas not installed. Some examples will be skipped.")

# Configure logging for better debugging
configure_logging(level="INFO")

# Example stream ID - replace with an actual stream ID from your account
SAMPLE_STREAM_ID = "23532363"  # Replace with your stream ID


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def export_stream_to_file(stream, temp_dir):
    """Demonstrate exporting stream data to various file formats."""
    print_separator("Exporting Stream to Files")

    # Create file paths in the temp directory
    json_path = Path(temp_dir) / "stream_data.json"
    jsonl_path = Path(temp_dir) / "stream_data.jsonl"

    # 1. Export to JSON format
    print(f"1. Exporting to JSON: {json_path}")
    start_time = time.time()
    stream.export_to_file(
        json_path,
        limit=1000,  # Limit to 1000 records for the example
        include_metadata=True)
    json_size = json_path.stat().st_size / (1024 * 1024)  # Size in MB
    end_time = time.time()
    print(f"   Export completed in {end_time - start_time:.2f} seconds")
    print(f"   File size: {json_size:.2f} MB")

    # 2. Export to JSON Lines format
    print(f"\n2. Exporting to JSON Lines: {jsonl_path}")
    start_time = time.time()
    stream.export_to_file(
        jsonl_path,
        limit=1000,  # Limit to 1000 records for the example
        file_format="jsonl",
        include_metadata=True)
    jsonl_size = jsonl_path.stat().st_size / (1024 * 1024)  # Size in MB
    end_time = time.time()
    print(f"   Export completed in {end_time - start_time:.2f} seconds")
    print(f"   File size: {jsonl_size:.2f} MB")

    # 4. Export to DataFrame formats if pandas is available
    if PANDAS_AVAILABLE:
        # Export to CSV
        csv_path = Path(temp_dir) / "stream_data.csv"
        print(f"\n4. Exporting to CSV: {csv_path}")
        start_time = time.time()
        stream.export_to_file(csv_path,
                              limit=1000,
                              file_format="csv",
                              include_metadata=True)
        csv_size = csv_path.stat().st_size / (1024 * 1024)  # Size in MB
        end_time = time.time()
        print(f"   Export completed in {end_time - start_time:.2f} seconds")
        print(f"   File size: {csv_size:.2f} MB")


    # Compare file sizes
    print("\nFile size comparison:")
    print(f"  JSON:     {json_size:.2f} MB")
    print(f"  JSONL:    {jsonl_size:.2f} MB")

    if PANDAS_AVAILABLE:
        if 'csv_size' in locals():
            print(f"  CSV:      {csv_size:.2f} MB")

    return {
        "json":
            json_path,
        "jsonl":
            jsonl_path,
        "csv":
            csv_path if PANDAS_AVAILABLE and 'csv_size' in locals() else None,
    }


def load_data_from_files(file_paths, stream):
    """Demonstrate loading data from previously exported files."""
    print_separator("Loading Data from Files")

    # 1. Load from JSON
    if file_paths.get("json"):
        print(f"\n1. Loading from JSON: {file_paths['json']}")
        start_time = time.time()
        json_data, json_metadata = stream.load_from_file(file_paths["json"])
        end_time = time.time()
        print(
            f"   Loaded {len(json_data)} records in {end_time - start_time:.2f} seconds"
        )
        print(f"   Metadata available: {json_metadata is not None}")

        # Show sample data
        if json_data:
            print("   First record sample:")
            first_record = json_data[0]
            sample_fields = list(first_record.keys())[:3]  # Show first 3 fields
            for field in sample_fields:
                print(f"     {field}: {first_record[field]}")

    # 2. Load from JSONL
    if file_paths.get("jsonl"):
        print(f"\n2. Loading from JSON Lines: {file_paths['jsonl']}")
        start_time = time.time()
        jsonl_data, jsonl_metadata = stream.load_from_file(file_paths["jsonl"])
        end_time = time.time()
        print(
            f"   Loaded {len(jsonl_data)} records in {end_time - start_time:.2f} seconds"
        )
        print(f"   Metadata available: {jsonl_metadata is not None}")


    # 4. Load from CSV (if pandas is available)
    if PANDAS_AVAILABLE and file_paths.get("csv"):
        print(f"\n4. Loading from CSV: {file_paths['csv']}")
        start_time = time.time()
        csv_data, csv_metadata = stream.load_from_file(file_paths["csv"])
        end_time = time.time()
        print(
            f"   Loaded {len(csv_data)} records in {end_time - start_time:.2f} seconds"
        )
        print(f"   Metadata available: {csv_metadata is not None}")

        # Convert to DataFrame
        if csv_data:
            df = pd.DataFrame(csv_data)
            print(f"   Converted to DataFrame with shape: {df.shape}")


def work_offline_mode(client, temp_dir, stream_id):
    """Demonstrate using DataStream in offline mode."""
    print_separator("Offline Mode")

    # Create a DataStream in offline mode
    # Note: This will use the cached file if available, otherwise fetch from API
    offline_stream = client.streams.from_id(stream_id)

    print(f"Created offline stream for ID: {offline_stream.stream_id}")
    print(f"Cache directory: {offline_stream._cache_dir}")

    # Check if we already have a cached file
    cache_file = offline_stream._get_cache_file_path()
    if cache_file.exists():
        print(f"Found existing cache file: {cache_file}")
    else:
        print("No cache file found, will create one on first data access")

    # Get data - this will either:
    # 1. Use cached file if it exists, or
    # 2. Fetch from API and create cache file for future use
    print("\nGetting data in offline mode...")
    start_time = time.time()
    data = offline_stream.get_data(limit=100)
    end_time = time.time()

    print(
        f"Retrieved {len(data)} records in {end_time - start_time:.2f} seconds")

    # Check if cache file was created
    if cache_file.exists():
        print(f"Cache file created/used: {cache_file}")
        print(f"Cache file size: {cache_file.stat().st_size / 1024:.2f} KB")

    # Get data again - should use cache
    print("\nGetting data again (should use cache)...")
    start_time = time.time()
    data_from_cache = offline_stream.get_data(limit=100)
    end_time = time.time()

    print(
        f"Retrieved {len(data_from_cache)} records in {end_time - start_time:.2f} seconds"
    )

    # Force live mode to bypass cache
    print("\nForcing live mode to bypass cache...")
    start_time = time.time()
    live_data = offline_stream.get_data(limit=100, force_live=True)
    end_time = time.time()

    print(
        f"Retrieved {len(live_data)} records in {end_time - start_time:.2f} seconds"
    )


def file_based_stream_example(file_path):
    """Demonstrate creating a DataStream directly from a file."""
    print_separator("File-Based DataStream")

    print(f"Creating DataStream from file: {file_path}")

    # Create a DataStream directly from a file without a client
    file_stream = DataStream.from_file(file_path)

    print(f"Stream key: {file_stream.stream_key}")

    # Access metadata if available
    if hasattr(file_stream, "_metadata") and file_stream._metadata:
        metadata = file_stream._metadata
        print("Stream metadata:")
        print(f"  Name: {metadata.display_name}")
        print(f"  Records: {metadata.meta.entry_count}")
        print(f"  Columns: {metadata.meta.main_property_count}")
    else:
        print("No metadata available")

    # Get data from the file
    print("\nReading data from file...")
    start_time = time.time()
    data = file_stream.get_data(limit=10)
    end_time = time.time()

    print(
        f"Retrieved {len(data)} records in {end_time - start_time:.2f} seconds")

    if data:
        print("\nSample data (first record):")
        first_record = data[0]
        sample_fields = list(first_record.keys())[:3]  # Show first 3 fields
        for field in sample_fields:
            print(f"  {field}: {first_record[field]}")


def main():
    """Run the offline mode examples."""
    # Check for API key
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("Error: FUSIONBASE_API_KEY environment variable not set.")
        print(
            "Please set your API key with: export FUSIONBASE_API_KEY=your_api_key_here"
        )
        return

    # Create a temporary directory for exported files
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory for examples: {temp_dir}")

        try:
            # Create client using context manager for proper resource handling
            with Fusionbase(api_key=api_key) as client:
                print_separator("DataStream Offline Mode Examples")

                # Check if the sample stream ID has been set
                if SAMPLE_STREAM_ID == "12345":
                    print(
                        "⚠️  Please replace SAMPLE_STREAM_ID with an actual stream ID!"
                    )
                    print(
                        "Attempting to continue with example ID, which may fail."
                    )

                # Initialize stream
                stream = client.streams.from_id(SAMPLE_STREAM_ID)

                # Run examples
                try:
                    # Export data to various file formats
                    exported_files = export_stream_to_file(stream, temp_dir)

                    # Load data from exported files
                    load_data_from_files(exported_files, stream)

                    # Demonstrate offline mode
                    work_offline_mode(client, temp_dir, SAMPLE_STREAM_ID)

                    # Create a DataStream directly from file
                    if exported_files.get("json"):
                        file_based_stream_example(exported_files["json"])

                    print_separator("Offline Mode Examples Complete")

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
