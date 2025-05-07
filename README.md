# Fusionbase: Fast access to the world's data

[![Pypi_Version](https://img.shields.io/pypi/v/fusionbase.svg)](https://pypi.python.org/pypi/fusionbase)
[![📦 PyPI](https://github.com/FusionbaseHQ/fusionbase-python/actions/workflows/publish-to-pypi.yml/badge.svg)](https://github.com/FusionbaseHQ/fusionbase-python/actions/workflows/publish-to-pypi.yml)
[![Tests](https://github.com/FusionbaseHQ/fusionbase-python/actions/workflows/tests.yml/badge.svg)](https://github.com/FusionbaseHQ/fusionbase-python/actions/workflows/tests.yml)

We believe that working with data, specifically the exploration and integration part, should be fun! Therefore, our API and Python package is designed to seamlessly support a data scientist's and engineer's daily work.

If you have questions, feel free to reach out!

The [Fusionbase](https://fusionbase.com/) python package is open source software released by Fusionbase's Engineering Team. It is available for download on [PyPI](https://pypi.python.org/pypi/fusionbase/).

## Important links

- Homepage: https://fusionbase.com
- HTML documentation: https://developer.fusionbase.com/fusionbase-api/
- Issue tracker: https://github.com/FusionbaseHQ/fusionbase/issues
- Source code repository: https://github.com/FusionbaseHQ/fusionbase
- Contributing: Reach out to us! info@fusionbase.com
- Fusionbase Python package: https://pypi.python.org/pypi/fusionbase/

## Installation

### Using pip (recommended)

```bash
pip install fusionbase
```

### Using Poetry

```bash
poetry add fusionbase
```

### Optional Dependencies

Install with specific features:

```bash
# For pandas integration
pip install "fusionbase[pandas]"

# For msgpack support (faster serialization)
pip install "fusionbase[msgpack]"

# For all optional dependencies
pip install "fusionbase[all]"
```

### Development Installation

```bash
git clone https://github.com/FusionbaseHQ/fusionbase-python.git
cd fusionbase-python
pip install -e ".[dev]"
```

## Quick Start

### Initialize the Client

```python
from fusionbase import Fusionbase

# Initialize with API key from environment variable FUSIONBASE_API_KEY
client = Fusionbase()

# Or provide API key explicitly
client = Fusionbase(api_key="your-api-key-here")

# Recommended: Use with context manager for proper resource handling
with Fusionbase() as client:
    # Your code here
    pass
```

### Working with Data Streams

```python
# Get a data stream by ID
stream = client.streams.from_id("your-stream-id")

# Get metadata about the stream
metadata = stream.get_metadata()
print(f"Stream name: {metadata.display_name}")
print(f"Total records: {metadata.meta.entry_count}")

# Get data (with optional pagination)
data = stream.get_data(limit=100)

# Convert directly to pandas DataFrame
df = stream.get_data(limit=100, return_type="dataframe")

# Create filters
price_filter = stream.create_filter("price", "GREATER_THAN", 100)
data = stream.get_data(filters=[price_filter], limit=50)
```

### Working with Entities

```python
# Get an organization by ID
org = client.entities.organizations.from_id("organization-id")
print(f"Organization name: {org.name}")
print(f"Website: {org.primary_website}")

# Get a person by ID
person = client.entities.persons.from_id("person-id")
print(f"Person name: {person.name}")

# Get a location by ID
location = client.entities.locations.from_id("location-id")
print(f"Location: {location.name}")
```

### Searching for Data

```python
# Search for organizations
org_results = client.search.organizations.search(q="Acme Corp", limit=5)
print(f"Found {len(org_results.items)} organizations")

# Search for persons
person_results = client.search.persons.search(q="John Doe", limit=5)
print(f"Found {len(person_results.items)} persons")

# Search for locations
location_results = client.search.locations.search(q="Berlin", limit=5)
print(f"Found {len(location_results.items)} locations")
```

### Async Support

```python
import asyncio

async def main():
    # Use the client asynchronously
    async with Fusionbase() as client:
        # Get metadata asynchronously
        metadata = await stream.aget_metadata()

        # Get data asynchronously
        data = await stream.aget_data(limit=100)

        # Search asynchronously
        results = await client.search.organizations.asearch(q="Acme Corp")

# Run the async function
asyncio.run(main())
```

## Examples

For more detailed examples, check the [examples directory](https://github.com/FusionbaseHQ/fusionbase-python/tree/main/examples):

## Changelog

### Version 1.0.0 (2025.05.07)
- Major version release with completely revamped architecture
- Full support for Fusionbase entity types (Organizations, Persons, Locations, etc.)
- New search functionality with type hinting and specialized search managers
- Improved async support throughout the entire API
- Enhanced pandas integration with chunk processing for large datasets
- New data filtering, sorting and pagination capabilities
- Simplified API with consistent patterns across all resource types
- Improved error handling and more informative error messages
- Comprehensive type annotations for better IDE support
- Performance optimizations for large data processing
- New offline mode for DataStream operations

### Version 0.2.9 (2024.01.13)
- orjson is now installed as dependency

### Version 0.2.8 (2023.02.14)
- Minor bug fixes and improvements

### Version 0.2.7 (2023.02.12)
- Added an option to leave the 'Auth' parameter None when creating a Fusionbase object if a corresponding environment variable (`FUSIONBASE_API_KEY`) is present

### Version 0.2.6 (2022.10.11)
- Some improvements

### Version 0.2.5 (2022.09.28)
- Performance improvements
- More flexible `limit` parameter

### Version 0.2.4 (2022.07.29)
- Minor improvements and additional test cases

### Version 0.2.3 (2022.07.28)
- Hotfix: Fix DataChunker import error

### Version 0.2.2 (2022.07.22)
- New methods to store data as files (json, csv and pickle)
- Improve logging features and completely new progress bar
- Major performance improvements
  - Leverage async and multiprocessing more
  - Add option to use `orjson` for faster json dumps
- Various bug fixes

### Version 0.2.1 (2022.06.15)
- Minor fixes and improvements

### Version 0.2.0 (2022.06.13)
- Feature: Add top-level authentication (breaking change)
- New API for invoking data services
- New caching method for data services

### Version 0.1.3 (2022.06.09)
- Bugfix: Skip and limit parameters now work as intended

### Version 0.1.2 (2022.06.07)
- Bugfix: Fix exception handling in update_create method
- Added tests for DataStream and DataService classes

### Version 0.1.1 (2022.05.12)
- Bugfix: `fields` parameter in `get_data` and `get_dataframe` works as intended now.

### Version 0.1.0 (2022.04.20)
- Initial release

## Contributing
Contributing to Fusionbase can be in contributions to the code base, sharing your experience and insights in the community on the Forums, or contributing to projects that make use of Fusionbase. Please see the [contributing guide](https://github.com/FusionbaseHQ/fusionbase-python/blob/main/docs/CONTRIBUTING.md) for more specifics.

## License
The Fusionbase python package is licensed under the [GPL 3](LICENSE).
