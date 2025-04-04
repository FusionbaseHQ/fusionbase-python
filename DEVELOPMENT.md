# Development Guide for Fusionbase Python Package

This document provides guidance for developers contributing to the Fusionbase Python package.

## Setting up the Development Environment

### Prerequisites

- Python 3.9+
- [Poetry](https://python-poetry.org/docs/#installation)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/FusionbaseHQ/fusionbase-python.git
cd fusionbase-python
```

2. Install dependencies with Poetry:
```bash
poetry install
```

This will create a virtual environment and install all dependencies, including development dependencies.

3. Activate the virtual environment:
```bash
poetry shell
```

4. Set up pre-commit hooks:
```bash
pre-commit install
```

## Development Workflow

### Pre-commit Hooks

We use pre-commit hooks to maintain code quality. These hooks run automatically when you try to commit changes:

- YAPF for code formatting
- isort for import sorting
- pylint for linting
- pytest for running tests
- Various file checks (trailing whitespace, merge conflicts, etc.)

You can run the hooks manually with:

```bash
pre-commit run --all-files
```

### Running Tests

To run tests:

```bash
poetry run pytest
```

To run tests with coverage:

```bash
poetry run pytest --cov=fusionbase
```

For tests that use real API access, you'll need to set an API key:

```bash
export FUSIONBASE_API_KEY=your_api_key
poetry run pytest
```

### Asynchronous Tests

We use pytest-asyncio for testing asynchronous code. The asyncio mode is set to "auto" in pyproject.toml, but you can also run specific async tests with:

```bash
poetry run pytest tests/path/to/async_test.py -v
```

## Code Style and Linting

We follow a consistent code style across the project enforced by various tools:

### Code Formatting

- **yapf**: Based on Google style with 4-space indentation and 99 character line limit
- **isort**: Uses Google profile with 88 character line limit

### Linting

- **pylint**: We maintain a minimum score of 7.5 out of 10
  - Only fails on errors (E), not warnings (W) or conventions (C)
  - Configured identically in both pyproject.toml and .pylintrc

### Testing

- **pytest**: Run with no header/summary, verbosely and with fail on first error

### Pre-commit Hooks

All these tools are configured as pre-commit hooks to ensure consistency. The same configurations are used whether running via pre-commit or directly through Poetry.

## Tool Configuration Reference

### pylint

- **fail-under**: 7.5 (allows commits with minor issues while maintaining quality)
- **fail-on**: E (only errors, not warnings or conventions)
- **line length**: 88 characters

### yapf

- **style**: Google
- **indent**: 4 spaces
- **line length**: 99 characters

### isort

- **profile**: Google
- **line length**: 88 characters

### pytest

- **options**: --no-header --no-summary -xvs

## Building and Publishing

### Building the Package

```bash
poetry build
```

This will create both source and wheel distributions in the `dist` directory.

### Publishing to PyPI

To publish a new version:

1. Update the version in pyproject.toml:
```bash
poetry version [patch|minor|major]
```

2. Commit the changes and create a tag that starts with 'v':
```bash
git add pyproject.toml
git commit -m "Bump version to x.y.z"
git tag vx.y.z
git push origin main --tags
```

The GitHub workflow will automatically publish the package to PyPI when a tag starting with 'v' is pushed to the repository.

Alternatively, you can publish manually:
```bash
poetry publish --username __token__ --password ${PYPI_API_TOKEN}
```

## Dependencies Management

When you need to add a new dependency:

```bash
# For runtime dependencies
poetry add package-name

# For development dependencies
poetry add --group dev package-name
```
