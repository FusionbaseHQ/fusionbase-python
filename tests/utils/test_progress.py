"""Tests for progress bar utilities."""

import io
import logging
import sys
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from fusionbase.utils.progress import create_chunk_iterator
from fusionbase.utils.progress import create_item_iterator
from fusionbase.utils.progress import RICH_AVAILABLE
from fusionbase.utils.progress import should_show_progress


class TestShouldShowProgress(unittest.TestCase):
    """Test cases for should_show_progress function."""

    def test_returns_false_when_not_tty(self):
        """Test returns False when stdout is not a TTY."""
        with patch.object(sys.stdout, 'isatty', return_value=False):
            result = should_show_progress()

            self.assertFalse(result)

    def test_returns_true_when_tty_and_info_level(self):
        """Test returns True when TTY and log level is INFO."""
        with patch.object(sys.stdout, 'isatty', return_value=True):
            root_logger = logging.getLogger()
            original_level = root_logger.level
            try:
                root_logger.setLevel(logging.INFO)
                result = should_show_progress()

                self.assertTrue(result)
            finally:
                root_logger.setLevel(original_level)

    def test_returns_true_when_tty_and_debug_level(self):
        """Test returns True when TTY and log level is DEBUG."""
        with patch.object(sys.stdout, 'isatty', return_value=True):
            root_logger = logging.getLogger()
            original_level = root_logger.level
            try:
                root_logger.setLevel(logging.DEBUG)
                result = should_show_progress()

                self.assertTrue(result)
            finally:
                root_logger.setLevel(original_level)

    def test_returns_false_when_tty_but_warning_level(self):
        """Test returns False when TTY but log level is WARNING or higher."""
        with patch.object(sys.stdout, 'isatty', return_value=True):
            root_logger = logging.getLogger()
            original_level = root_logger.level
            try:
                root_logger.setLevel(logging.WARNING)
                result = should_show_progress()

                self.assertFalse(result)
            finally:
                root_logger.setLevel(original_level)


class TestCreateChunkIterator(unittest.TestCase):
    """Test cases for create_chunk_iterator function."""

    def test_yields_all_chunks_when_no_progress(self):
        """Test iterator yields all chunks when progress is disabled."""
        chunks = [[1, 2], [3, 4], [5, 6]]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_chunk_iterator(iter(chunks)))

            self.assertEqual(result, chunks)

    def test_yields_all_chunks_when_rich_not_available(self):
        """Test iterator yields all chunks when rich is not available."""
        chunks = [[1, 2], [3, 4]]

        with patch('fusionbase.utils.progress.RICH_AVAILABLE', False):
            result = list(create_chunk_iterator(iter(chunks)))

            self.assertEqual(result, chunks)

    def test_preserves_chunk_contents(self):
        """Test iterator preserves chunk contents exactly."""
        chunks = [["a", "b"], ["c", "d", "e"]]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_chunk_iterator(iter(chunks)))

            self.assertEqual(result[0], ["a", "b"])
            self.assertEqual(result[1], ["c", "d", "e"])

    def test_handles_empty_iterable(self):
        """Test iterator handles empty iterable."""
        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_chunk_iterator(iter([])))

            self.assertEqual(result, [])

    def test_accepts_description_parameter(self):
        """Test iterator accepts description parameter."""
        chunks = [[1, 2]]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            # Should not raise
            result = list(create_chunk_iterator(
                iter(chunks),
                description="Custom description"
            ))

            self.assertEqual(result, chunks)

    def test_accepts_total_parameter(self):
        """Test iterator accepts total parameter."""
        chunks = [[1, 2], [3, 4]]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            # Should not raise
            result = list(create_chunk_iterator(
                iter(chunks),
                total=4
            ))

            self.assertEqual(result, chunks)


class TestCreateItemIterator(unittest.TestCase):
    """Test cases for create_item_iterator function."""

    def test_yields_all_items_when_no_progress(self):
        """Test iterator yields all items when progress is disabled."""
        items = [1, 2, 3, 4, 5]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_item_iterator(iter(items)))

            self.assertEqual(result, items)

    def test_yields_all_items_when_rich_not_available(self):
        """Test iterator yields all items when rich is not available."""
        items = ["a", "b", "c"]

        with patch('fusionbase.utils.progress.RICH_AVAILABLE', False):
            result = list(create_item_iterator(iter(items)))

            self.assertEqual(result, items)

    def test_preserves_item_values(self):
        """Test iterator preserves item values exactly."""
        items = [{"id": 1}, {"id": 2}]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_item_iterator(iter(items)))

            self.assertEqual(result[0]["id"], 1)
            self.assertEqual(result[1]["id"], 2)

    def test_handles_empty_iterable(self):
        """Test iterator handles empty iterable."""
        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_item_iterator(iter([])))

            self.assertEqual(result, [])

    def test_accepts_description_parameter(self):
        """Test iterator accepts description parameter."""
        items = [1, 2, 3]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_item_iterator(
                iter(items),
                description="Processing items"
            ))

            self.assertEqual(result, items)

    def test_accepts_total_parameter(self):
        """Test iterator accepts total parameter."""
        items = [1, 2, 3]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_item_iterator(
                iter(items),
                total=3
            ))

            self.assertEqual(result, items)


class TestRichAvailability(unittest.TestCase):
    """Test cases for RICH_AVAILABLE constant."""

    def test_rich_available_is_boolean(self):
        """Test RICH_AVAILABLE is a boolean."""
        self.assertIsInstance(RICH_AVAILABLE, bool)


class TestProgressIteratorIntegration(unittest.TestCase):
    """Integration tests for progress iterators."""

    def test_chunk_iterator_can_be_reused(self):
        """Test chunk iterator pattern can be used in loops."""
        chunks = [[1, 2], [3, 4], [5, 6]]
        processed = []

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            for chunk in create_chunk_iterator(iter(chunks)):
                processed.extend(chunk)

        self.assertEqual(processed, [1, 2, 3, 4, 5, 6])

    def test_item_iterator_can_be_reused(self):
        """Test item iterator pattern can be used in loops."""
        items = [1, 2, 3, 4, 5]
        total = 0

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            for item in create_item_iterator(iter(items)):
                total += item

        self.assertEqual(total, 15)

    def test_nested_iterators(self):
        """Test nested use of chunk and item iterators."""
        chunks = [[1, 2], [3, 4]]
        all_items = []

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            for chunk in create_chunk_iterator(iter(chunks)):
                for item in create_item_iterator(iter(chunk)):
                    all_items.append(item)

        self.assertEqual(all_items, [1, 2, 3, 4])


class TestProgressWithRichMocked(unittest.TestCase):
    """Test progress bars with rich mocked."""

    @unittest.skipIf(not RICH_AVAILABLE, "Rich not available")
    def test_chunk_iterator_with_rich_and_progress_disabled(self):
        """Test chunk iterator when rich available but progress disabled."""
        chunks = [[1, 2], [3, 4]]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_chunk_iterator(iter(chunks)))

            self.assertEqual(result, chunks)

    @unittest.skipIf(not RICH_AVAILABLE, "Rich not available")
    def test_item_iterator_with_rich_and_progress_disabled(self):
        """Test item iterator when rich available but progress disabled."""
        items = [1, 2, 3]

        with patch('fusionbase.utils.progress.should_show_progress', return_value=False):
            result = list(create_item_iterator(iter(items)))

            self.assertEqual(result, items)
