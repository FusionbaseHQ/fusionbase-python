"""Context management for Fusionbase SDK."""

import contextvars
from typing import Any, Optional

# Context variables to store the current client and entity manager
_current_client = contextvars.ContextVar("fusionbase_client", default=None)
_current_entity_manager = contextvars.ContextVar("fusionbase_entity_manager",
                                                 default=None)


def set_current_client(client: Any) -> contextvars.Token:
    """Set the current client in this execution context.

    Args:
        client: The client instance

    Returns:
        A token that can be used to restore the previous value
    """
    return _current_client.set(client)


def get_current_client() -> Optional[Any]:
    """Get the current client from this execution context.

    Returns:
        The current client or None if not set
    """
    return _current_client.get()


def set_current_entity_manager(manager: Any) -> contextvars.Token:
    """Set the current entity manager in this execution context.

    Args:
        manager: The entity manager instance

    Returns:
        A token that can be used to restore the previous value
    """
    return _current_entity_manager.set(manager)


def get_current_entity_manager() -> Optional[Any]:
    """Get the current entity manager from this execution context.

    Returns:
        The current entity manager or None if not set
    """
    return _current_entity_manager.get()
