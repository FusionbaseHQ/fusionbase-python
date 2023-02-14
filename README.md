# Fusionbase: Fast access to the world's data

<!-- !TODO -->
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

## Installation in Python - PyPI release

Fusionbase is on PyPI, so you can use `pip` to install it.

```bash
pip install fusionbase
```

If you want to use all features and be able to retrieve the data directly as pandas dataframes, you must make sure that pandas and numpy are installed.

```bash
pip install pandas
pip install numpy
```

Fusionbase by default uses the standard JSON library of Python to serialize and locally store data. However, you can use the faster `orjson` library as a drop-in replacement.&#x20;

Therefore, just install `orjson` and Fusionbase will automatically detect and use it.

```bash
pip install orjson
```

### Getting Started

Got to [examples](https://github.com/FusionbaseHQ/fusionbase-python/tree/main/examples) to deep dive into Fusionbase and see various examples on how to use the package.

Here are some Examples for a quick start:

#### Data Streams

The Data Stream module lets you conveniently access data and metadata of all Data Streams. Each stream can be accessed via its unique stream id or label.



**Setup**

```python
# Import Fusionbase
from fusionbase import Fusionbase

# Create a new datastream
# Provide your API Key
fusionbase = Fusionbase(auth={"api_key": "*** SECRET CREDENTIALS ***"})

# If you prefer to have extended logging output and information 
# Like a progress bar for downloading datastreams etc.
# Turn on the log 
fusionbase = Fusionbase(auth={"api_key": "*** SECRET CREDENTIALS ***"}, log=True)

# Get the datastream with the key "28654971"
data_stream_key = "28654971"
data_stream = fusionbase.get_datastream(data_stream_key)
```

****

**Human readable datastream information**

```python
# Print a nice table containing the meta data of the stream
data_stream.pretty_meta_data()
```

****

**Getting the data**

The samples below show how to retrieve the data of a datastream as a list of dictionaries. Each element in the list represents one row within the dataset.&#x20;

Note that the data can by hierarchical.

```python
# The following returns the full datastream as a list of dictionaries
# It uses a local cache if available
data = data_stream.get_data()
print(data)

# Get always the latest data from Fusionbase
data = data_stream.get_data(live=True)
print(data)

# If you only need a subset of the columns
# You'll gain much performance by only selecting those columns
data = data_stream.get_data(fields=["NAME_OF_COLUMN_1", "NAME_OF_COLUMN_N"])
print(data)

# If you need only an excerpt of the data you can use skip and limit
# The sample below gets the 10 first rows
data = data_stream.get_data(skip=0, limit=10)
print(data)

```

****

**Get Data as a** [**pandas**](https://pandas.pydata.org/) **DataFrame**

If you are working with pandas, it is probably the most convenient way to load to data directly as a pandas DataFrame.

```python
# Load the data from Fusionbase, cache it and put it in a pandas DataFrame
df = data_stream.as_dataframe()
print(df)

# Force ignoring the cache and make sure to get the latest data
df = data_stream.as_dataframe(live=True)
print(df)
```



**Storing the data**

Large datasets potentially do not fit into the memory. Therefore, it is possible to get the data of a stream directly as partitioned files.&#x20;

The folder structure is automatically created and always like `./{ID-OF-THE-STREAM}/data/*`

```python
from pathlib import Path

# Store as JSON files
data_stream.as_json_files(storage_path=Path("./data/"))

# Store as CSV files
data_stream.as_csv_files(storage_path=Path("./data/"))

# Store as Pickle files
data_stream.as_pickle_files(storage_path=Path("./data/"))
```

####

#### Data Services

A data service can be seen as an API that returns a certain output for a specific input. For example, our address [normalization service](https://app.fusionbase.com/share/25127186) parses an address and returns the structured and normalized parts of it.

**Setup**

```python
# Import Fusionbase
from fusionbase.Fusionbase import Fusionbase

# Create a new dataservice
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": "*** SECRET CREDENTIALS ***"})

data_service_key = "23622632"
data_service = fusionbase.get_dataservice(data_service_key)
```

**Human readable dataservice information:**

```python
# Retrieves the metadata from a Service by giving a Service specific key and prints it nicely to console
data_service.pretty_meta_data()
```

**Human readable dataservice definition:**

```python
# Retrieve the request definition (such as required parameters) from a Service by giving a Service specific key and print it to console.
data_service.pretty_request_definition()
```

**Invoke a dataservice:**

```python
# Invoke a service by providing input data

# The following lines of code are equivalent
# Services can be invoked directly by their parameter names
result = data_service.invoke(address_string="Agnes-Pockels-Bogen 1, 80992 München")


# Or using a list of parameter key and value pairs
payload = [
    {
        "name": "address_string",  # THIS IS THE NAME OF THE INPUT VALUE
        "value": "Agnes-Pockels-Bogen 1, 80992 München"  # THE VALUE FOR THE INPUT
    }
]
result = data_service.invoke(parameters=payload)

print(result)
```


## Changelog

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
