"""
Pandas integration with DataStream.

This example demonstrates how to work with DataStream data using pandas,
leveraging the native DataFrame support we've added.
"""

import os
import time

from fusionbase import Fusionbase
from fusionbase.core.logging import configure_logging
from fusionbase.data.datastream import FilterOperator

# Try importing pandas - we'll handle the case if it's not installed
try:
    import matplotlib.pyplot as plt
    import pandas as pd  # noqa: F401
    PANDAS_AVAILABLE = True
    MATPLOTLIB_AVAILABLE = True
except ImportError as e:
    if "pandas" in str(e):
        PANDAS_AVAILABLE = False
        MATPLOTLIB_AVAILABLE = False
        print(
            "Pandas not installed. Run 'pip install pandas' to enable pandas features."
        )
    elif "matplotlib" in str(e):
        PANDAS_AVAILABLE = True
        MATPLOTLIB_AVAILABLE = False
        print(
            "Matplotlib not installed. Run 'pip install matplotlib' to enable plotting."
        )

# Configure logging for better debugging
configure_logging(level="INFO")

# Example stream ID - replace with an actual stream ID from your account
SAMPLE_STREAM_ID = "23532363"  # Replace with your stream ID


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def basic_dataframe_conversion(stream):
    """Demonstrate basic conversion of stream data to pandas DataFrame."""
    print_separator("Basic DataFrame Conversion")

    if not PANDAS_AVAILABLE:
        print("Skipping: pandas is not installed.")
        return

    # Method 1: Using to_pandas()
    print("1. Using to_pandas() method:")
    start = time.time()
    df = stream.get_data(limit=10, return_type="dataframe")
    end = time.time()

    print(f"   Converted to DataFrame in {end - start:.2f} seconds")
    print(f"   DataFrame shape: {df.shape}")
    print("\nDataFrame head:")
    print(df.head(3))


def filtered_dataframes(stream):
    """Demonstrate getting filtered data as DataFrames."""
    print_separator("Filtered DataFrames")

    if not PANDAS_AVAILABLE:
        print("Skipping: pandas is not installed.")
        return

    # Get metadata to find fields to use
    metadata = stream.get_metadata()
    columns = metadata.column_names

    # Choose a field to filter on (first column as example)
    filter_field = columns[0]

    # Get sample data to use for filter value
    sample = stream.get_data(limit=1)[0]
    filter_value = sample.get(filter_field)

    print(f"Creating filter on field {filter_field} = {filter_value}")

    # Create filter
    equals_filter = stream.create_filter(filter_field, FilterOperator.EQUALS,
                                         filter_value)

    # Get filtered DataFrame
    print("Getting filtered DataFrame...")
    df = stream.get_data(
        limit=10,
        return_type="dataframe",
        filters=[equals_filter],
    )

    print(f"DataFrame shape: {df.shape}")
    print("\nFiltered DataFrame head:")
    print(df.head(3))


def chunked_dataframe_processing(stream):
    """Demonstrate processing large datasets in DataFrame chunks."""
    print_separator("Chunked DataFrame Processing")

    if not PANDAS_AVAILABLE:
        print("Skipping: pandas is not installed.")
        return

    print("Processing data in DataFrame chunks...")

    # Process chunks of data as DataFrames
    total_rows = 0

    # Use iter_chunks_pandas to get chunks as DataFrames
    for i, df_chunk in enumerate(stream.iter_chunks_pandas(chunk_size=100), 1):
        print(
            f"Processing chunk {i}: {df_chunk.shape[0]} rows, {df_chunk.shape[1]} columns"
        )

        # Example transformation: calculate means of numeric columns
        numeric_cols = df_chunk.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print("  Numeric column means:")
            for col in numeric_cols[:3]:  # Show first 3 numeric columns
                print(f"    {col}: {df_chunk[col].mean()}")

        total_rows += df_chunk.shape[0]

        # Only process 3 chunks for the example
        if i >= 3:
            print("\nStopping after 3 chunks for demonstration purposes.")
            break

    print(f"\nProcessed {total_rows} total rows in {i} chunks")


def dataframe_visualization(stream):
    """Demonstrate visualizing stream data with pandas and matplotlib."""
    print_separator("DataFrame Visualization")

    if not PANDAS_AVAILABLE:
        print("Skipping: pandas is not installed.")
        return

    if not MATPLOTLIB_AVAILABLE:
        print("Skipping: matplotlib is not installed.")
        return

    # Get a DataFrame to visualize
    df = stream.get_data(limit=1000, return_type="dataframe")

    # Find numeric columns to visualize
    numeric_cols = df.select_dtypes(include=['number']).columns

    if len(numeric_cols) >= 1:
        # Choose a numeric column for visualization
        column_to_plot = numeric_cols[0]

        print(f"Creating basic visualization for column: {column_to_plot}")

        # Create a simple histogram
        plt.figure(figsize=(10, 6))
        df[column_to_plot].hist(bins=20)
        plt.title(f'Histogram of {column_to_plot}')
        plt.xlabel(column_to_plot)
        plt.ylabel('Frequency')

        # Save the figure instead of displaying (to avoid blocking in examples)
        plt.savefig('datastream_histogram.png')
        print("Histogram saved as 'datastream_histogram.png'")

        # Create a second visualization if we have multiple numeric columns
        if len(numeric_cols) >= 2:
            col1 = numeric_cols[0]
            col2 = numeric_cols[1]

            plt.figure(figsize=(10, 6))
            plt.scatter(df[col1], df[col2], alpha=0.5)
            plt.title(f'Scatter plot: {col1} vs {col2}')
            plt.xlabel(col1)
            plt.ylabel(col2)

            plt.savefig('datastream_scatter.png')
            print("Scatter plot saved as 'datastream_scatter.png'")
    else:
        print("No numeric columns found for visualization.")


def dataframe_analysis(stream):
    """Demonstrate basic data analysis with pandas DataFrames."""
    print_separator("DataFrame Analysis")

    if not PANDAS_AVAILABLE:
        print("Skipping: pandas is not installed.")
        return

    # Get data as DataFrame
    df = stream.get_data(limit=1000, return_type="dataframe")

    print("DataFrame summary statistics:")
    print(df.describe())

    # Show column data types
    print("\nDataFrame data types:")
    print(df.dtypes)

    # Show nulls/missing values
    print("\nMissing values per column:")
    print(df.isnull().sum())

    # Basic groupby example if we have categorical columns
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns

    if len(categorical_cols) > 0:
        group_col = categorical_cols[0]  # Use first categorical column

        # Get numeric columns to aggregate
        numeric_cols = df.select_dtypes(include=['number']).columns

        if len(numeric_cols) > 0:
            print(
                f"\nGrouping by {group_col} and calculating mean for numeric columns:"
            )
            grouped = df.groupby(group_col)[numeric_cols].mean()
            print(grouped.head())


def main():
    """Run the pandas integration examples."""
    # Check for pandas availability
    if not PANDAS_AVAILABLE:
        print("Error: pandas is required for these examples.")
        print("Please install pandas with: pip install pandas")
        return

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
            print_separator("DataStream Pandas Integration Examples")

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
                basic_dataframe_conversion(stream)
                filtered_dataframes(stream)
                chunked_dataframe_processing(stream)
                dataframe_visualization(stream)
                dataframe_analysis(stream)

                print_separator("Pandas Integration Examples Complete")

            except Exception as e:
                print(f"Error in example: {e}")
                print(
                    "If this is due to an invalid stream ID, please update the SAMPLE_STREAM_ID."
                )

    except Exception as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()
