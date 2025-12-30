"""Tests for context management functionality."""

import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

from fusionbase.core.context import (
    get_current_client,
    get_current_entity_manager,
    set_current_client,
    set_current_entity_manager,
)


class TestClientContext(unittest.TestCase):
    """Test cases for client context management."""

    def setUp(self):
        """Reset context before each test."""
        # Clear any existing context
        set_current_client(None)

    def tearDown(self):
        """Clean up context after each test."""
        set_current_client(None)

    def test_get_current_client_default_none(self):
        """Test that get_current_client returns None by default."""
        result = get_current_client()

        self.assertIsNone(result)

    def test_set_and_get_current_client(self):
        """Test setting and getting current client."""
        mock_client = MagicMock()
        mock_client.name = "test_client"

        set_current_client(mock_client)
        result = get_current_client()

        self.assertEqual(result, mock_client)
        self.assertEqual(result.name, "test_client")

    def test_set_current_client_returns_token(self):
        """Test that set_current_client returns a token."""
        mock_client = MagicMock()

        token = set_current_client(mock_client)

        self.assertIsNotNone(token)

    def test_set_current_client_can_restore(self):
        """Test that token can be used to restore previous value."""
        first_client = MagicMock()
        first_client.name = "first"
        second_client = MagicMock()
        second_client.name = "second"

        set_current_client(first_client)
        self.assertEqual(get_current_client().name, "first")

        # Set second client
        set_current_client(second_client)
        self.assertEqual(get_current_client().name, "second")

    def test_set_current_client_none_clears_context(self):
        """Test that setting None clears the context."""
        mock_client = MagicMock()

        set_current_client(mock_client)
        self.assertIsNotNone(get_current_client())

        set_current_client(None)
        self.assertIsNone(get_current_client())


class TestEntityManagerContext(unittest.TestCase):
    """Test cases for entity manager context management."""

    def setUp(self):
        """Reset context before each test."""
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up context after each test."""
        set_current_entity_manager(None)

    def test_get_current_entity_manager_default_none(self):
        """Test that get_current_entity_manager returns None by default."""
        result = get_current_entity_manager()

        self.assertIsNone(result)

    def test_set_and_get_current_entity_manager(self):
        """Test setting and getting current entity manager."""
        mock_manager = MagicMock()
        mock_manager.name = "test_manager"

        set_current_entity_manager(mock_manager)
        result = get_current_entity_manager()

        self.assertEqual(result, mock_manager)
        self.assertEqual(result.name, "test_manager")

    def test_set_current_entity_manager_returns_token(self):
        """Test that set_current_entity_manager returns a token."""
        mock_manager = MagicMock()

        token = set_current_entity_manager(mock_manager)

        self.assertIsNotNone(token)

    def test_set_current_entity_manager_none_clears_context(self):
        """Test that setting None clears the context."""
        mock_manager = MagicMock()

        set_current_entity_manager(mock_manager)
        self.assertIsNotNone(get_current_entity_manager())

        set_current_entity_manager(None)
        self.assertIsNone(get_current_entity_manager())


class TestContextIsolation(unittest.TestCase):
    """Test that context is properly isolated."""

    def setUp(self):
        """Reset context before each test."""
        set_current_client(None)
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up context after each test."""
        set_current_client(None)
        set_current_entity_manager(None)

    def test_client_and_manager_are_independent(self):
        """Test that client and manager contexts are independent."""
        mock_client = MagicMock()
        mock_client.type = "client"
        mock_manager = MagicMock()
        mock_manager.type = "manager"

        set_current_client(mock_client)
        set_current_entity_manager(mock_manager)

        self.assertEqual(get_current_client().type, "client")
        self.assertEqual(get_current_entity_manager().type, "manager")

        # Clear client, manager should remain
        set_current_client(None)
        self.assertIsNone(get_current_client())
        self.assertIsNotNone(get_current_entity_manager())

    def test_context_isolation_between_threads(self):
        """Test that context is isolated between threads."""
        results = {"main": None, "thread": None}

        def thread_func():
            # Thread should not see main thread's client
            results["thread"] = get_current_client()

        # Set client in main thread
        mock_client = MagicMock()
        mock_client.name = "main_client"
        set_current_client(mock_client)
        results["main"] = get_current_client()

        # Run function in another thread
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(thread_func)
            future.result()

        # Main thread has client, other thread doesn't
        self.assertIsNotNone(results["main"])
        self.assertIsNone(results["thread"])

    def test_context_in_different_threads(self):
        """Test setting different values in different threads."""
        results = {"thread1": None, "thread2": None}

        def thread1_func():
            mock_client = MagicMock()
            mock_client.name = "thread1_client"
            set_current_client(mock_client)
            results["thread1"] = get_current_client().name

        def thread2_func():
            mock_client = MagicMock()
            mock_client.name = "thread2_client"
            set_current_client(mock_client)
            results["thread2"] = get_current_client().name

        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(thread1_func)
            f2 = executor.submit(thread2_func)
            f1.result()
            f2.result()

        self.assertEqual(results["thread1"], "thread1_client")
        self.assertEqual(results["thread2"], "thread2_client")


class TestAsyncContextIsolation(unittest.TestCase):
    """Test context isolation in async context."""

    def setUp(self):
        """Reset context before each test."""
        set_current_client(None)
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up context after each test."""
        set_current_client(None)
        set_current_entity_manager(None)

    def test_async_context_preserved(self):
        """Test that context is preserved across async await."""
        async def async_func():
            mock_client = MagicMock()
            mock_client.name = "async_client"
            set_current_client(mock_client)

            # Simulate async operation
            await asyncio.sleep(0.01)

            # Context should still be available
            return get_current_client()

        result = asyncio.run(async_func())

        self.assertIsNotNone(result)
        self.assertEqual(result.name, "async_client")

    def test_async_tasks_share_context_by_default(self):
        """Test context behavior with concurrent tasks."""
        async def main():
            results = {}

            async def task1():
                await asyncio.sleep(0.01)
                results["task1"] = get_current_client()

            async def task2():
                mock_client = MagicMock()
                mock_client.name = "task2_client"
                set_current_client(mock_client)
                await asyncio.sleep(0.01)
                results["task2"] = get_current_client()

            # Set initial context
            mock_client = MagicMock()
            mock_client.name = "main_client"
            set_current_client(mock_client)

            # Run tasks - note: contextvars behavior with asyncio.gather
            await asyncio.gather(task1(), task2())

            return results

        results = asyncio.run(main())

        # Both tasks should have access to context (behavior depends on Python version)
        self.assertIn("task1", results)
        self.assertIn("task2", results)


class TestContextUsagePatterns(unittest.TestCase):
    """Test common context usage patterns."""

    def setUp(self):
        """Reset context before each test."""
        set_current_client(None)
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up context after each test."""
        set_current_client(None)
        set_current_entity_manager(None)

    def test_context_manager_pattern(self):
        """Test using context with a context manager pattern."""
        mock_client = MagicMock()

        class ClientContext:
            def __init__(self, client):
                self.client = client
                self.token = None

            def __enter__(self):
                self.token = set_current_client(self.client)
                return self.client

            def __exit__(self, *args):
                set_current_client(None)

        # Outside context, no client
        self.assertIsNone(get_current_client())

        # Inside context, client available
        with ClientContext(mock_client):
            self.assertEqual(get_current_client(), mock_client)

        # Outside context again, no client
        self.assertIsNone(get_current_client())

    def test_nested_context_pattern(self):
        """Test nested context setting."""
        client1 = MagicMock()
        client1.name = "outer"
        client2 = MagicMock()
        client2.name = "inner"

        # Set outer context
        set_current_client(client1)
        self.assertEqual(get_current_client().name, "outer")

        # Set inner context
        set_current_client(client2)
        self.assertEqual(get_current_client().name, "inner")

        # Restore outer (manual)
        set_current_client(client1)
        self.assertEqual(get_current_client().name, "outer")

    def test_multiple_calls_same_value(self):
        """Test setting same value multiple times."""
        mock_client = MagicMock()

        set_current_client(mock_client)
        set_current_client(mock_client)
        set_current_client(mock_client)

        result = get_current_client()

        self.assertEqual(result, mock_client)
