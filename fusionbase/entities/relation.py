"""Relation entity module."""

import asyncio
from datetime import datetime
import inspect
from typing import Any, ClassVar, List, Optional, Type

import httpx
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from fusionbase.entities.base import Entity
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.types.entities import EntityType


class LocalizedText(BaseModel):
    """Text with language codes."""

    model_config = ConfigDict(extra="allow")
    en: Optional[str] = None
    de: Optional[str] = None


class ParameterDefinition(BaseModel):
    """Definition of a parameter for relation resolving."""

    name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[LocalizedText] = None
    required: bool = False
    default: Optional[Any] = None


class RelationResolveConfig(BaseModel):
    """Configuration for resolving relations."""

    parameter_definition: List[ParameterDefinition] = []


class RelationMeta(BaseModel):
    """Metadata for relations."""

    relation_start_date: Optional[datetime] = None
    relation_end_date: Optional[datetime] = None


class Relation(Entity):
    """Relation entity in Fusionbase.

    Relations represent connections between entities in the Fusionbase system.

    Attributes:
        name: The localized name of the relation
        description: Localized description of the relation
        label: The type label of the relation (e.g., ENTITY_NETWORK, BOARD_MEMBER_OF)
        model_from: The entity type where the relation starts
        model_to: The entity type where the relation ends
        meta: Additional metadata about the relation
        resolve: Configuration for resolving the relation
    """

    # Direct API response fields
    name: Optional[LocalizedText] = None
    description: Optional[LocalizedText] = None
    label: Optional[str] = None
    model_from: Optional[EntityType] = None
    model_to: Optional[EntityType] = None
    meta: Optional[RelationMeta] = None
    resolve: Optional[RelationResolveConfig] = None

    # Entity type information
    entity_type: ClassVar[EntityType] = EntityType.RELATION

    # Default for fb_entity_version to handle search results
    fb_entity_version: str = ""

    @model_validator(mode='before')
    @classmethod
    def process_entity_types(cls, data):
        """Process entity types from strings to EntityType enum."""
        if isinstance(data, dict):
            # Convert model_from string to EntityType enum
            if 'model_from' in data and isinstance(data['model_from'], str):
                try:
                    # Try to convert from lowercase or uppercase string
                    model_from = data['model_from'].lower()
                    for entity_type in EntityType:
                        if entity_type.value == model_from or entity_type.name == data[
                                'model_from']:
                            data['model_from'] = entity_type
                            break
                except (ValueError, AttributeError):
                    # If conversion fails, keep as is - will be validated by Pydantic
                    pass

            # Convert model_to string to EntityType enum
            if 'model_to' in data and isinstance(data['model_to'], str):
                try:
                    # Try to convert from lowercase or uppercase string
                    model_to = data['model_to'].lower()
                    for entity_type in EntityType:
                        if entity_type.value == model_to or entity_type.name == data[
                                'model_to']:
                            data['model_to'] = entity_type
                            break
                except (ValueError, AttributeError):
                    # If conversion fails, keep as is - will be validated by Pydantic
                    pass

        return data

    @property
    def relation_name(self) -> Optional[str]:
        """Get the relation name in English or German."""
        if not self.name:
            return self.label
        if self.name.en:
            return self.name.en
        if self.name.de:
            return self.name.de
        return self.label

    @property
    def relation_description(self) -> Optional[str]:
        """Get the relation description in English or German."""
        if not self.description:
            return None
        if self.description.en:
            return self.description.en
        if self.description.de:
            return self.description.de
        return None

    @property
    def relation_id(self) -> str:
        """Get the numeric relation ID."""
        if self.fb_entity_id.startswith("relations/"):
            return self.fb_entity_id.replace("relations/", "")
        return self.fb_entity_id

    def get_from_entity_class(self) -> Optional[Type[Entity]]:
        """Get the entity class for the model_from type.

        Returns:
            The entity class corresponding to model_from, or None if unavailable
        """
        if not self.model_from:
            return None

        # Import here to avoid circular imports
        from fusionbase.entities.event import Event
        from fusionbase.entities.feature import Feature
        from fusionbase.entities.location import Location
        from fusionbase.entities.organization import Organization
        from fusionbase.entities.person import Person

        # Map entity types to their classes
        entity_map = {
            EntityType.ORGANIZATION: Organization,
            EntityType.PERSON: Person,
            EntityType.LOCATION: Location,
            EntityType.EVENT: Event,
            EntityType.FEATURE: Feature
        }

        return entity_map.get(self.model_from)

    def get_to_entity_class(self) -> Optional[Type[Entity]]:
        """Get the entity class for the model_to type.

        Returns:
            The entity class corresponding to model_to, or None if unavailable
        """
        if not self.model_to:
            return None

        # Import here to avoid circular imports
        from fusionbase.entities.event import Event
        from fusionbase.entities.feature import Feature
        from fusionbase.entities.location import Location
        from fusionbase.entities.organization import Organization
        from fusionbase.entities.person import Person

        # Map entity types to their classes
        entity_map = {
            EntityType.ORGANIZATION: Organization,
            EntityType.PERSON: Person,
            EntityType.LOCATION: Location,
            EntityType.EVENT: Event,
            EntityType.FEATURE: Feature
        }

        return entity_map.get(self.model_to)

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Relation":
        """Internal method to create a Relation instance by fetching it from the API.

        Args:
            client: The Fusionbase client
            entity_id: ID of the relation to fetch

        Returns:
            A Relation instance

        Raises:
            ResourceNotFoundError: If the relation doesn't exist
            AuthenticationError: If authentication fails
            AuthorizationError: If the user is not authorized
            APIError: For other API errors
        """
        # Use the request method with retry if available
        if hasattr(client, "request"):
            try:
                data = client.request("GET", f"relation/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to relations
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("relation", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("relation", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        elif hasattr(client, "make_request"):
            # If client is an EntityManager
            try:
                data = client.make_request(f"relation/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to relations
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("relation", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("relation", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        else:
            try:
                response = client.http_client.get(f"relation/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError as e:
                # Use our error parser to generate appropriate exceptions
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("relation", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:  # pylint: disable=broad-except
                if getattr(e, "response", None) is not None:
                    raise parse_error_response(getattr(e, "response")) from e

                raise APIError(
                    f"Failed to retrieve relation (ID: {entity_id}): {e}",
                    500,
                ) from e

        # Convert the API response to match our entity model
        relation_data = {
            "fb_entity_id": data.get("id") or f"relations/{data.get('key')}",
            "fb_entity_version":
                "",  # Relations don't seem to have version info
            "name": data.get("name"),
            "description": data.get("description"),
            "label": data.get("label"),
            "model_from": data.get("model_from"),
            "model_to": data.get("model_to"),
            "meta": data.get("meta"),
            "resolve": data.get("resolve"),
            "metadata": {
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
            },
        }

        return cls.model_validate(relation_data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Relation":
        """Create Relation instance by async fetching."""
        try:
            data = None
            # Try direct async methods on the client
            if hasattr(client, "aget"):
                data = await client.aget(f"relation/get/{entity_id}")
            # Use arequest method if available
            elif hasattr(client, "arequest"):
                data = await client.arequest("GET", f"relation/get/{entity_id}")
            # Use amake_request method if available (for entity managers)
            elif hasattr(client, "amake_request"):
                data = await client.amake_request(f"relation/get/{entity_id}")
            # Use async HTTP client directly
            elif hasattr(client, "_async_http_client"):
                response = await client._async_http_client.get(
                    f"relation/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            else:
                # Fall back to sync method through asyncio.to_thread
                if hasattr(client,
                           "request") and not inspect.iscoroutinefunction(
                               client.request):
                    data = await asyncio.to_thread(cls._from_id, client,
                                                   entity_id)
                    return data  # Return early as we already have a Relation instance

                # No suitable async method found, raise an error
                raise APIError(
                    f"No suitable async method found to fetch relation (ID: {entity_id})",
                    status_code=500)

            # Make sure we have data
            if not data:
                raise APIError(
                    f"Failed to retrieve relation data (ID: {entity_id})",
                    status_code=500)

            # Convert the API response to match our entity model
            relation_data = {
                "fb_entity_id":
                    data.get("id") or f"relations/{data.get('key')}",
                "fb_entity_version":
                    "",  # Relations don't seem to have version info
                "name":
                    data.get("name"),
                "description":
                    data.get("description"),
                "label":
                    data.get("label"),
                "model_from":
                    data.get("model_from"),
                "model_to":
                    data.get("model_to"),
                "meta":
                    data.get("meta"),
                "resolve":
                    data.get("resolve"),
                "metadata": {
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at"),
                },
            }

            return cls.model_validate(relation_data)

        except ResourceNotFoundError as e:
            # Make the error more specific to relations
            response = getattr(e, "response", None)
            raise ResourceNotFoundError("relation", entity_id, response) from e
        except httpx.HTTPStatusError as e:
            if getattr(e, "response", None) is not None:
                if getattr(e.response, "status_code", None) == 404:
                    raise ResourceNotFoundError("relation", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            raise APIError(("Failed to retrieve relation "
                            f"(ID: {entity_id}): {e}"), 500) from e
