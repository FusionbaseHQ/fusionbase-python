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

## Getting Started

Got to [examples](https://github.com/FusionbaseHQ/fusionbase-python/tree/main/examples) to deep dive into Fusionbase and see various examples on how to use the package.

Here are some Examples for a quick start:

### Data Streams
The Data Stream module lets you conveniently access data and metadata of all Data Streams. Each stream can be accessed via its unique stream id or label.

**Setup**
```python
# Import Fusionbase
from fusionbase.Fusionbase import Fusionbase

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

data_stream_key = "28654971"
data_stream = fusionbase.get_datastream(data_stream_key)
```

**Human readable datastream information:**
```python
# Prints a nice table containing the meta data of the stream
data_stream.pretty_meta_data()
```

**Get Data:**
```python
data = data_stream.get_data()
print(data)
```

**Get Data as a [pandas](https://pandas.pydata.org/) Dataframe:**
```python
df = data_stream.get_dataframe()
print(df)
```

### Data Services
A data service can be seen as an API that returns a certain output for a specific input. 
For example, our address [normalization service](https://app.fusionbase.com/share/25127186) parses an address and returns the structured and normalized parts of it.

**Setup**
```python
# Import Fusionbase
from fusionbase.Fusionbase import Fusionbase

# Create a new dataservice
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

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
# Retrieves the request definition (such as required parameters) from a Service by giving a Service specific key and print it to console.
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
