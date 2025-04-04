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

### Code Formatting

We use YAPF (following Google style guide) and isort for code formatting:

```bash
# Format with YAPF
poetry run yapf -i -r fusionbase/ tests/

# Sort imports
poetry run isort .
```

### Code Linting

We use Pylint with Google style guide for linting:

```bash
poetry run pylint fusionbase
poetry run pylint tests
```

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
