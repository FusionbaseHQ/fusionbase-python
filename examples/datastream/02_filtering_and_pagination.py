"""
Filtering and pagination with DataStream.

This example demonstrates how to use filters, pagination,
and sorting with DataStream.
"""

import os

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.data.datastream import FilterOperator

# Configure logging for better debugging
configure_logging(level="INFO")

# Example stream ID - replace with an actual stream ID from your account
SAMPLE_STREAM_ID = "23532363"  # Replace with your stream ID


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def pagination_example(stream):
    """Demonstrate paginating through stream data."""
    print_separator("Pagination")

    # Get the first page (5 records)
    print("First page (records 1-5):")
    page_1 = stream.get_data(skip=0, limit=5)

    for i, record in enumerate(page_1, 1):
        # Print just the first field to keep output manageable
        first_field = next(iter(record.keys()))
        print(f"  Record {i}: {first_field}={record[first_field]}")

    # Get the second page (next 5 records)
    print("\nSecond page (records 6-10):")
    page_2 = stream.get_data(skip=5, limit=5)

    for i, record in enumerate(page_2, 6):
        # Print just the first field to keep output manageable
        first_field = next(iter(record.keys()))
        print(f"  Record {i}: {first_field}={record[first_field]}")

    print("\nDifferent page sizes:")
    # Get 3 records starting from position 8
    custom_page = stream.get_data(skip=8, limit=3)

    for i, record in enumerate(custom_page, 9):
        # Print just the first field to keep output manageable
        first_field = next(iter(record.keys()))
        print(f"  Record {i}: {first_field}={record[first_field]}")


def sorting_example(stream):
    """Demonstrate sorting stream data."""
    print_separator("Sorting")

    # First, get metadata to find a numeric or date field to sort by
    metadata = stream.get_metadata()
    column_names = metadata.column_names

    # Choose a field to sort by (for demonstration)
    # In a real scenario, you'd choose an appropriate field
    sort_field = column_names[0]  # Just use the first column
    print(f"Sorting by field: {sort_field}")

    # Sort in ascending order
    print("\nAscending order:")
    asc_data = stream.get_data(limit=3,
                               sort_keys=[sort_field],
                               sort_order=["asc"])

    for i, record in enumerate(asc_data, 1):
        print(f"  Record {i}: {sort_field}={record.get(sort_field)}")

    # Sort in descending order
    print("\nDescending order:")
    desc_data = stream.get_data(limit=3,
                                sort_keys=[sort_field],
                                sort_order=["desc"])

    for i, record in enumerate(desc_data, 1):
        print(f"  Record {i}: {sort_field}={record.get(sort_field)}")

    # Multi-field sorting if we have at least 2 columns
    if len(column_names) >= 2:
        print("\nMulti-field sorting:")
        multi_sort_data = stream.get_data(
            limit=3,
            sort_keys=[column_names[0], column_names[1]],
            sort_order=["asc", "desc"])

        for i, record in enumerate(multi_sort_data, 1):
            print(
                f"  Record {i}: {column_names[0]}={record.get(column_names[0])}, "
                f"{column_names[1]}={record.get(column_names[1])}")


def filtering_example(stream):
    """Demonstrate filtering stream data."""
    print_separator("Filtering")

    # Get metadata to find fields to filter on
    metadata = stream.get_metadata()
    column_names = metadata.column_names

    # Show available fields
    print("Available fields for filtering:")
    for name in column_names[:5]:  # Show first 5 fields
        print(f"  - {name}")

    # Choose a field to filter on (for demonstration)
    # In a real scenario, you'd choose an appropriate field
    filter_field = column_names[0]  # Just use the first column

    print(f"\nFiltering with field: {filter_field}")

    # Get a sample record to determine a value to filter on
    sample = stream.get_data(limit=1)[0]
    filter_value = sample.get(filter_field)

    print(f"Filter value: {filter_value}")

    # Simple equals filter
    print("\n1. Equals filter:")
    print(f"   Finding records where {filter_field} = {filter_value}")

    equals_filter = stream.create_filter(filter_field, FilterOperator.EQUALS,
                                         filter_value)

    equals_results = stream.get_data(filters=[equals_filter], limit=3)
    print(f"   Found {len(equals_results)} records")

    for i, record in enumerate(equals_results, 1):
        print(f"   Record {i}: {filter_field}={record.get(filter_field)}")

    # Enum-based filter construction
    print("\n2. Using FilterOperator enum:")
    print("   Creating a NOT_EQUALS filter")

    not_equals_filter = stream.create_filter(filter_field,
                                             FilterOperator.NOT_EQUALS,
                                             filter_value)

    not_equals_results = stream.get_data(filters=[not_equals_filter], limit=3)
    print(f"   Found {len(not_equals_results)} records")

    # Multiple filters (if we have more than one column)
    if len(column_names) >= 2:
        print("\n3. Multiple filters:")
        field2 = column_names[1]
        sample.get(field2)

        print(f"   Finding records where {filter_field} = {filter_value}")
        print(f"   AND {field2} is not null")

        # Create filters
        filter1 = stream.create_filter(filter_field, FilterOperator.EQUALS,
                                       filter_value)
        filter2 = stream.create_filter(field2, FilterOperator.IS_NOT_NULL, None)

        multi_filter_results = stream.get_data(filters=[filter1, filter2],
                                               limit=3)
        print(f"   Found {len(multi_filter_results)} records")


def field_projection_example(stream):
    """Demonstrate projecting specific fields."""
    print_separator("Field Projection")

    # Get metadata to find fields
    metadata = stream.get_metadata()
    column_names = metadata.column_names

    # Choose a subset of fields (for demonstration)
    if len(column_names) >= 3:
        projection_fields = column_names[:3]  # First 3 fields
    else:
        projection_fields = column_names  # All fields if less than 3

    print(f"Projecting only these fields: {projection_fields}")

    # Get data with only the specified fields
    projected_data = stream.get_data(project_fields=projection_fields, limit=3)

    for i, record in enumerate(projected_data, 1):
        print(f"\nRecord {i}:")
        for field in projection_fields:
            print(f"  {field}: {record.get(field)}")

    # Note the fields that were excluded
    print(
        "\nThe records only include the projected fields, other fields are excluded."
    )


def main():
    """Run the filtering and pagination examples."""
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
            print_separator("DataStream Filtering and Pagination Examples")

            # Check if the sample stream ID has been set
            if SAMPLE_STREAM_ID == "12345":
                print(
                    "⚠️  Please replace SAMPLE_STREAM_ID with an actual stream ID!"
                )
                print("Attempting to continue with example ID, which may fail.")

            # Initialize stream
            stream = client.streams.from_id(SAMPLE_STREAM_ID)

            # Run examples
            try:
                pagination_example(stream)
                sorting_example(stream)
                filtering_example(stream)
                field_projection_example(stream)

                print_separator("Filtering and Pagination Examples Complete")

            except Exception as e:
                print(f"Error in example: {e}")
                print(
                    "If this is due to an invalid stream ID, please update the SAMPLE_STREAM_ID."
                )

    except Exception as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
