"""
Configuration options for the Fusionbase SDK.

This example demonstrates different ways to configure the SDK.
"""

from fusionbase import Fusionbase
from fusionbase import FusionbaseConfig
from fusionbase.config import CacheConfig
from fusionbase.config import LoggingConfig
from fusionbase.config import RetryConfig
from fusionbase.logging import configure_logging

# Configure basic logging
configure_logging(level="INFO")

# pylint: disable=no-member


def print_separator(title):
    """Print a section separator."""
    print(f"\n=== {title} ===")


def main():
    """Run the configuration examples."""
    print_separator("Fusionbase SDK Configuration Examples")

    # Default configuration
    print_separator("Default Configuration")
    default_config = FusionbaseConfig()
    # Access instance properties directly
    cache_enabled = default_config.cache.enabled  # Access the property directly
    print("Default timeout:", default_config.timeout, "seconds")
    print("Default cache settings:", cache_enabled)
    print("Default retry settings:", default_config.retry.enabled)

    # Basic configuration with constructor parameters
    print_separator("Basic Configuration")
    basic_config = FusionbaseConfig(
        timeout=30.0,  # 30-second timeout
        cache={
            "enabled": True,
            "ttl_seconds": 1800
        },  # 30-minute cache
        retry={
            "enabled": True,
            "max_attempts": 3
        },  # Retry up to 3 times
        max_connections=5,  # Maximum of 5 concurrent connections
    )
    # Access instance properties directly
    ttl_value = basic_config.cache.ttl_seconds
    attempts_value = basic_config.retry.max_attempts
    print("Custom timeout:", basic_config.timeout, "seconds")
    print("Custom cache TTL:", ttl_value, "seconds")
    print("Custom retry attempts:", attempts_value)
    print("Custom max connections:", basic_config.max_connections)

    # Detailed configuration with specific config objects
    print_separator("Detailed Configuration")
    detailed_config = FusionbaseConfig(
        timeout=15.0,
        cache=CacheConfig(
            enabled=True,
            ttl_seconds=600,  # 10 minutes
            directory="./custom_cache",  # Custom cache directory
            size_limit=100_000_000  # 100MB size limit
        ),
        retry=RetryConfig(enabled=True,
                          max_attempts=5,
                          min_wait_seconds=0.5,
                          max_wait_seconds=10.0,
                          retry_statuses=[500, 502, 503, 504],
                          retry_exceptions=["ConnectionError", "TimeoutError"]),
        logging=LoggingConfig(level="INFO",
                              log_requests=True,
                              hide_sensitive_data=True,
                              format="{time} | {level} | {message}"),
        max_connections=10)

    # Access instance properties directly
    directory = detailed_config.cache.directory
    size_limit = detailed_config.cache.size_limit
    retry_statuses = detailed_config.retry.retry_statuses
    print("Cache directory:", directory)
    print("Cache size limit:", size_limit, "bytes")
    print("Retry status codes:", retry_statuses)

    # Split into multiple lines for readability
    min_wait = detailed_config.retry.min_wait_seconds
    max_wait = detailed_config.retry.max_wait_seconds
    wait_config = f"Min wait: {min_wait}, Max wait: {max_wait}"
    print("Retry wait time range:", wait_config)

    log_format = detailed_config.logging.format
    print("Log format:", log_format)

    # Create a client with custom config
    print_separator("Creating Client with Custom Config")
    try:
        client = Fusionbase(config=detailed_config)
        print("✓ Client created with custom configuration")
        client.close()
    except ValueError as e:
        print(f"✗ Error creating client: {e}")
        print("  Make sure to set the FUSIONBASE_API_KEY environment variable.")

    print_separator("Configuration Examples Complete")


if __name__ == "__main__":
    main()
