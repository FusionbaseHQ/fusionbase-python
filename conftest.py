"""
Global pytest configuration.
This file registers custom markers and other pytest configurations.
"""


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers",
                            "asyncio: mark a test as an asyncio coroutine")
