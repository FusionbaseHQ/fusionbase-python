"""Base entity module for Fusionbase entities."""

from typing import Any, ClassVar, Dict, Optional

from pydantic import BaseModel
from pydantic import ConfigDict

from fusionbase.core.context import get_current_entity_manager
from fusionbase.types.entities import EntityType


class Entity(BaseModel):
    """Base class for all Fusionbase entities.

    All specific entity types (Organization, Person, etc.) inherit from this class.

    Attributes:
        fb_entity_id: Unique identifier for the entity in the Fusionbase system
        fb_entity_version: Version identifier for the entity
        name: Name of the entity
        metadata: Additional metadata about the entity
        external_ids: Dictionary of identifiers from external systems
    """

    # Enable arbitrary types for better subclassing support
    model_config = ConfigDict(arbitrary_types_allowed=True)

    fb_entity_id: str
    fb_entity_version: str
    name: Optional[str] = None
    metadata: Dict[str, Any] = {}
    external_ids: Dict[str, Any] = {}

    # Class variables to store entity type information
    entity_type: ClassVar[
        EntityType] = EntityType.ORGANIZATION  # Default, will be overridden

    @classmethod
    def get(cls, entity_id: str) -> "Entity":
        """Get an entity instance by ID using the current entity manager.

        This is a convenience method that leverages the entity manager
        associated with the current Fusionbase client context.

        Args:
            entity_id: ID of the entity to fetch

        Returns:
            An instance of the appropriate entity type

        Raises:
            RuntimeError: If no entity manager is available in the context
            APIError: If the entity cannot be retrieved
        """

        manager = get_current_entity_manager()
        if manager is None:
            raise RuntimeError(
                "No entity manager available in current context. Use with statement: "
                "with fusionbase.Client() as client: ...")
        return manager.get(cls.entity_type.value, entity_id)

    @classmethod
    async def aget(cls, entity_id: str) -> "Entity":
        """Asynchronously get an entity instance by ID using the current entity manager.

        This is a convenience method that leverages the entity manager
        associated with the current Fusionbase client context.

        Args:
            entity_id: ID of the entity to fetch

        Returns:
            An instance of the appropriate entity type

        Raises:
            RuntimeError: If no entity manager is available in the context
            APIError: If the entity cannot be retrieved
        """
        manager = get_current_entity_manager()
        if manager is None:
            raise RuntimeError(
                "No entity manager available in current context. Use with statement: "
                "with fusionbase.Client() as client: ...")
        return await manager.afrom_id(cls.entity_type.value, entity_id)

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Entity":
        """Internal method to create an entity instance by fetching it from the API.

        This is meant to be implemented by subclasses and not called directly.

        Args:
            client: The Fusionbase client
            entity_id: ID of the entity to fetch

        Returns:
            An instance of the appropriate entity type

        Raises:
            APIError: If the entity cannot be retrieved
        """
        raise NotImplementedError(
            "The _from_id method must be implemented by subclasses")
