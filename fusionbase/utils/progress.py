"""Progress bar utilities for long-running operations."""

import logging
import sys
from typing import Iterator, List, Optional, TypeVar

# Define a type variable for generic iterator types
T = TypeVar("T")

# Try to import rich for fancy progress bars
try:
    from rich.progress import BarColumn
    from rich.progress import Progress
    from rich.progress import SpinnerColumn
    from rich.progress import TextColumn
    from rich.progress import TimeElapsedColumn
    from rich.progress import TimeRemainingColumn
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Create a logger for this module
logger = logging.getLogger(__name__)


def should_show_progress() -> bool:
    """Determine if we should show a progress bar based on log level and terminal.

    Returns:
        True if we should show a progress bar, False otherwise
    """
    # Only show progress bars in interactive terminals
    if not sys.stdout.isatty():
        return False

    # Check the log level - show progress if level is INFO or lower (more verbose)
    root_logger = logging.getLogger()
    return root_logger.level <= logging.INFO


def create_chunk_iterator(
    iterable: Iterator[List[T]],
    total: Optional[int] = None,
    description: str = "Processing chunks",
) -> Iterator[List[T]]:
    """Create an iterator with a progress bar for chunks.

    Args:
        iterable: Source iterator of chunks
        total: Total expected number of items (not chunks)
        chunk_size: Size of each chunk (used to calculate progress)
        description: Description to show in the progress bar

    Returns:
        Iterator that displays progress
    """
    # If rich is not available or we shouldn't show progress, return the original iterator
    if not RICH_AVAILABLE or not should_show_progress():
        yield from iterable
        return

    # Initialize rich progress bar
    with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            TextColumn("[blue]{task.fields[processed_chunks]} chunks"),
    ) as progress:
        # If total is unknown, use spinner-only style
        if total is None:
            task_id = progress.add_task(description,
                                        total=None,
                                        processed_chunks=0)
            processed_chunks = 0
            processed_items = 0

            # Iterate through chunks, updating progress
            for chunk in iterable:
                processed_chunks += 1
                processed_items += len(chunk)
                progress.update(task_id,
                                advance=1,
                                processed_chunks=processed_chunks)
                yield chunk
        else:
            # We know the total, so show a real progress bar
            task_id = progress.add_task(description,
                                        total=total,
                                        processed_chunks=0)
            processed_chunks = 0
            processed_items = 0

            # Iterate through chunks, updating progress based on actual items processed
            for chunk in iterable:
                processed_chunks += 1
                processed_items += len(chunk)
                progress.update(task_id,
                                completed=processed_items,
                                processed_chunks=processed_chunks)
                yield chunk


def create_item_iterator(
    iterable: Iterator[T],
    total: Optional[int] = None,
    description: str = "Processing items",
) -> Iterator[T]:
    """Create an iterator with a progress bar for individual items.

    Args:
        iterable: Source iterator
        total: Total expected number of items
        description: Description to show in the progress bar

    Returns:
        Iterator that displays progress
    """
    # If rich is not available or we shouldn't show progress, return the original iterator
    if not RICH_AVAILABLE or not should_show_progress():
        yield from iterable
        return

    # Initialize rich progress bar
    with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
    ) as progress:
        # Create task
        task_id = progress.add_task(description, total=total)

        # Iterate through items, updating progress
        for item in iterable:
            progress.update(task_id, advance=1)
            yield item
