"""Base entity module for Fusionbase entities."""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, TYPE_CHECKING

from pydantic import BaseModel
from pydantic import ConfigDict

from fusionbase.core.context import get_current_entity_manager
from fusionbase.types.entities import EntityType

if TYPE_CHECKING:
    from fusionbase.entities.relation import Relation


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

        This method is designed to be overridden by subclasses that need custom
        handling, but provides a default implementation that works for most entities.
        """
        # Import here to avoid circular import
        from fusionbase.utils.api_utils import fetch_entity_sync
        return fetch_entity_sync(cls, client, entity_id)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Entity":
        """Asynchronously create an entity instance by fetching it from the API.

        This method is designed to be overridden by subclasses that need custom
        handling, but provides a default implementation that works for most entities.
        """
        # Import here to avoid circular import
        from fusionbase.utils.api_utils import fetch_entity_async
        return await fetch_entity_async(cls, client, entity_id)

    def get_relations(self, client=None) -> List["Relation"]:
        """Get all relations available for this entity type.

        Args:
            client: Optional Fusionbase client. If not provided, will attempt to use the
                   current client context.

        Returns:
            A list of Relation instances representing available relations

        Raises:
            APIError: If the relations cannot be retrieved
            RuntimeError: If no client is provided and no client is available in context
        """
        if client is None:
            manager = get_current_entity_manager()
            if manager is None:
                raise RuntimeError(
                    "No client provided and no entity manager available in current context. "
                    "Use with statement: with fusionbase.Client() as client: ..."
                )
            client = manager.client

        return self.__class__.list_relations(client)

    async def aget_relations(self, client=None) -> List["Relation"]:
        """Asynchronously get all relations available for this entity type.

        Args:
            client: Optional Fusionbase client. If not provided, will attempt to use the
                   current client context.

        Returns:
            A list of Relation instances representing available relations

        Raises:
            APIError: If the relations cannot be retrieved
            RuntimeError: If no client is provided and no client is available in context
        """
        if client is None:
            manager = get_current_entity_manager()
            if manager is None:
                raise RuntimeError(
                    "No client provided and no entity manager available in current context. "
                    "Use with statement: with fusionbase.Client() as client: ..."
                )
            client = manager.client

        return await self.__class__.alist_relations(client)

    @classmethod
    def list_relations(cls, client) -> List["Relation"]:
        """List all relations available for this entity type.

        Args:
            client: The Fusionbase client

        Returns:
            A list of Relation instances representing available relations

        Raises:
            APIError: If the relations cannot be retrieved
        """
        from fusionbase.entities.relation import Relation

        # Use the entity_type value to determine the endpoint path
        entity_type_value = cls.entity_type.value

        # Make the API request
        if hasattr(client, "request"):
            response_data = client.request(
                "GET", f"relation/list/{entity_type_value}")
        else:
            response = client.http_client.get(
                f"relation/list/{entity_type_value}")
            response.raise_for_status()
            response_data = response.json()

        # Convert the response data to Relation instances
        relations = []
        for relation_data in response_data:
            # Convert API response to the expected structure
            relation_mapping = {
                "fb_entity_id":
                    relation_data.get("id",
                                      f"relations/{relation_data.get('key')}"),
                "name":
                    relation_data.get("name"),
                "description":
                    relation_data.get("description"),
                "label":
                    relation_data.get("label"),
                "model_from":
                    relation_data.get("model_from"),
                "model_to":
                    relation_data.get("model_to"),
                "meta":
                    relation_data.get("meta"),
                "resolve":
                    relation_data.get("resolve"),
                "metadata": {
                    "created_at": relation_data.get("created_at"),
                    "updated_at": relation_data.get("updated_at"),
                }
            }
            relations.append(Relation.model_validate(relation_mapping))

        return relations

    @classmethod
    async def alist_relations(cls, client) -> List["Relation"]:
        """Asynchronously list all relations available for this entity type.

        Args:
            client: The Fusionbase client

        Returns:
            A list of Relation instances representing available relations

        Raises:
            APIError: If the relations cannot be retrieved
        """
        import asyncio
        import inspect

        from fusionbase.entities.relation import Relation

        entity_type_value = cls.entity_type.value

        # Use appropriate async client method if available
        if hasattr(client, "arequest"):
            response_data = await client.arequest(
                "GET", f"relation/list/{entity_type_value}")
        elif hasattr(client, "aget"):
            response_data = await client.aget(
                f"relation/list/{entity_type_value}")
        elif hasattr(client, "_async_http_client"):
            response = await client._async_http_client.get(
                f"relation/list/{entity_type_value}")
            response.raise_for_status()
            response_data = response.json()
        else:
            # Fall back to sync method through asyncio.to_thread if client's request method is not async
            if hasattr(client, "request") and not inspect.iscoroutinefunction(
                    client.request):
                return await asyncio.to_thread(cls.list_relations, client)

            # No suitable async method found, raise an error
            from fusionbase.exceptions import APIError
            raise APIError(
                f"No suitable async method found to fetch relations for {entity_type_value}",
                status_code=500)

        # Convert the response data to Relation instances
        relations = []
        for relation_data in response_data:
            # Convert API response to the expected structure
            relation_mapping = {
                "fb_entity_id":
                    relation_data.get("id",
                                      f"relations/{relation_data.get('key')}"),
                "name":
                    relation_data.get("name"),
                "description":
                    relation_data.get("description"),
                "label":
                    relation_data.get("label"),
                "model_from":
                    relation_data.get("model_from"),
                "model_to":
                    relation_data.get("model_to"),
                "meta":
                    relation_data.get("meta"),
                "resolve":
                    relation_data.get("resolve"),
                "metadata": {
                    "created_at": relation_data.get("created_at"),
                    "updated_at": relation_data.get("updated_at"),
                }
            }
            relations.append(Relation.model_validate(relation_mapping))

        return relations
