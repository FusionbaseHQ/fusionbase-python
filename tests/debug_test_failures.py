"""Test troubleshooting script to identify failing tests."""

import traceback

import pytest


def run_tests_individually():
    """Run each test individually to identify which one is failing."""
    from _pytest.config import Config
    from _pytest.runner import pytest_runtest_protocol

    config = Config.fromdictargs({}, [])
    config.pluginmanager.hook.pytest_configure(config=config)

    # Get all test files
    test_files = [
        "tests/entities/test_location.py",
        "tests/entities/test_person.py",
        "tests/search/test_location_search.py",
        "tests/search/test_person_search.py",
        "tests/test_cache.py",
        "tests/test_client.py",
    ]

    print("Running tests individually to find failures...\n")

    for test_file in test_files:
        print(f"Testing file: {test_file}")
        try:
            pytest.main(["-xvs", test_file])
        except Exception as e:
            print(f"Error running {test_file}: {e}")
            traceback.print_exc()
        print("\n" + "-" * 80 + "\n")


if __name__ == "__main__":
    run_tests_individually()
