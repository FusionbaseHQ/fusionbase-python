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

### Getting Started

Got to [examples](https://github.com/FusionbaseHQ/fusionbase-python/tree/main/examples) to deep dive into Fusionbase and see various examples on how to use the package.


## Changelog
### Version 0.2.9 (2024.01.13)
- orjson is now installed as dependeny

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
