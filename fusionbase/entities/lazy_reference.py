"""Lazy entity reference module."""

import sys
import traceback
from typing import Any, Callable, Generic, Optional, Type, TypeVar

T = TypeVar('T')


class LazyReference(Generic[T]):
    """A lazy reference to an entity that is loaded only when accessed.

    This class implements a cursor-like reference system where the actual entity
    is only fetched from the API when needed.
    """

    def __init__(self,
                 entity_id: str,
                 entity_cls: Type[T],
                 client: Any,
                 loader_func: Optional[Callable] = None,
                 async_loader_func: Optional[Callable] = None):
        """Initialize a lazy reference.

        Args:
            entity_id: The ID of the entity to load
            entity_cls: The class of the entity
            client: The Fusionbase client to use for loading
            loader_func: Optional custom function to load the entity
            async_loader_func: Optional custom async function to load the entity
        """
        self._entity_id = entity_id
        self._entity_cls = entity_cls
        self._client = client
        self._loaded_entity = None
        self._loader_func = loader_func
        self._async_loader_func = async_loader_func
        self._load_error = None

    @property
    def entity_id(self) -> str:
        """Get the entity ID."""
        return self._entity_id

    @property
    def is_loaded(self) -> bool:
        """Check if the entity has been loaded."""
        return self._loaded_entity is not None

    def get(self) -> T:
        """Load and return the entity if not already loaded."""
        if self._load_error:
            raise self._load_error

        if not self._loaded_entity:
            try:
                if self._loader_func:
                    self._loaded_entity = self._loader_func(
                        self._client, self._entity_id)
                else:
                    # Print debug info in tests
                    if 'pytest' in sys.modules and self._entity_cls.__name__ in [
                            "Person", "Location"
                    ]:
                        print(
                            f"DEBUG: Loading {self._entity_cls.__name__} with ID {self._entity_id}"
                        )

                    self._loaded_entity = self._entity_cls._from_id(
                        self._client, self._entity_id)
            except Exception as e:
                self._load_error = e
                print(
                    f"Error loading entity {self._entity_cls.__name__} with ID {self._entity_id}: {e}"
                )
                print(traceback.format_exc())
                raise

        return self._loaded_entity

    async def aget(self) -> T:
        """Asynchronously load and return the entity if not already loaded."""
        if self._load_error:
            raise self._load_error

        if not self._loaded_entity:
            try:
                if self._async_loader_func:
                    self._loaded_entity = await self._async_loader_func(
                        self._client, self._entity_id)
                else:
                    self._loaded_entity = await self._entity_cls._afrom_id(
                        self._client, self._entity_id)
            except Exception as e:
                self._load_error = e
                print(
                    f"Error loading entity {self._entity_cls.__name__} with ID {self._entity_id}: {e}"
                )
                print(traceback.format_exc())
                raise

        return self._loaded_entity

    def __getattr__(self, name):
        """Transparently pass attribute access to the loaded entity."""
        return getattr(self.get(), name)

    def __str__(self) -> str:
        """Return a string representation of the reference."""
        if self._loaded_entity:
            return f"LazyReference(loaded: {str(self._loaded_entity)})"
        return f"LazyReference(unloaded: {self._entity_cls.__name__}:{self._entity_id})"

    def __repr__(self) -> str:
        """Return a representation of the reference."""
        if self._loaded_entity:
            return f"LazyReference(loaded: {repr(self._loaded_entity)})"
        return f"LazyReference(unloaded: {self._entity_cls.__name__}:{self._entity_id})"
