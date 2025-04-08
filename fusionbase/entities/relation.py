"""Relation entity module."""

import asyncio
from datetime import datetime
import inspect
from typing import Any, ClassVar, Dict, List, Optional, Type, Union

import httpx
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from fusionbase.entities.base import Entity
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.exceptions import ValidationError
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

    def validate_value(self, value: Any) -> Any:
        """Validate a parameter value against this definition.

        Args:
            value: The value to validate

        Returns:
            The validated value (possibly converted to the correct type)

        Raises:
            ValidationError: If validation fails
        """
        if self.name is None:
            return value  # Can't validate without a name

        # Check if required parameter is missing
        if self.required and value is None:
            raise ValidationError(f"Parameter '{self.name}' is required")

        # If value is None but parameter has default, use default
        if value is None and self.default is not None:
            return self.default

        # If value is None and not required, it's valid
        if value is None and not self.required:
            return None

        # Validate type based on the 'type' field (if provided)
        if self.type is not None and value is not None:
            try:
                # Map string types to Python types
                # TODO: verify if these are the correct types in the api or not !!
                type_map = {
                    "string": str,
                    "str": str,
                    "integer": int,
                    "int": int,
                    "float": float,
                    "boolean": bool,
                    "bool": bool,
                    "array": list,
                    "list": list,
                    "object": dict,
                    "dict": dict
                }

                # Get the expected Python type
                expected_type = type_map.get(self.type.lower())
                if expected_type and not isinstance(value, expected_type):
                    # Try to convert value to expected type
                    return expected_type(value)
            except (ValueError, TypeError) as e:
                raise ValidationError(
                    f"Parameter '{self.name}' has invalid type. Expected {self.type}, got {type(value).__name__}"
                ) from e

        return value


class RelationResolveConfig(BaseModel):
    """Configuration for resolving relations."""

    parameter_definition: List[ParameterDefinition] = []

    def validate_parameters(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters against the parameter definitions.

        Args:
            parameters: Parameters to validate

        Returns:
            Validated parameters

        Raises:
            ValidationError: If validation fails
        """
        if not self.parameter_definition:
            return parameters  # No definitions to validate against

        validated_params = {}

        # First check all required parameters are present
        for param_def in self.parameter_definition:
            if param_def.name is None:
                continue

            param_name = param_def.name
            param_value = parameters.get(param_name)

            # Validate the parameter value
            validated_value = param_def.validate_value(param_value)

            # Only add non-None values to keep the request payload clean
            if validated_value is not None:
                validated_params[param_name] = validated_value

        return validated_params


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
        resolve_config: Configuration for resolving the relation
    """

    # Direct API response fields
    name: Optional[LocalizedText] = None
    description: Optional[LocalizedText] = None
    label: Optional[str] = None
    model_from: Optional[EntityType] = None
    model_to: Optional[EntityType] = None
    meta: Optional[RelationMeta] = None
    resolve_config: Optional[
        RelationResolveConfig] = None  # Renamed from resolve to resolve_config

    # Entity type information
    entity_type: ClassVar[EntityType] = EntityType.RELATION

    # Default for fb_entity_version to handle search results
    fb_entity_version: str = ""

    # Store client reference for resolving relations
    _client: Any = None

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

            # Handle the renamed attribute in the incoming data
            if 'resolve' in data:
                data['resolve_config'] = data.pop('resolve')

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
        """Get the entity class for the model_from type."""
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
        """Get the entity class for the model_to type."""
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
        """Internal method to create a Relation instance by fetching it from the API."""
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
            "resolve_config":
                data.get("resolve"),  # Renamed from resolve to resolve_config
            "metadata": {
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
            },
        }

        # Create the relation instance
        relation = cls.model_validate(relation_data)
        # Store the client for later use with resolve methods
        relation._client = client
        return relation

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
                "resolve_config":
                    data.get("resolve"
                            ),  # Renamed from resolve to resolve_config
                "metadata": {
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at"),
                },
            }

            # Create the relation instance
            relation = cls.model_validate(relation_data)
            # Store the client for later use with resolve methods
            relation._client = client
            return relation

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

    def resolve(self,
                entity: Union[Entity, str],
                parameters: Optional[Dict[str, Any]] = None,
                **kwargs) -> Any:
        """Resolve this relation with the given entity.

        Args:
            entity: An entity instance or entity ID to resolve the relation with
            parameters: Optional parameters required by the relation as a dictionary
            **kwargs: Additional parameters as keyword arguments

        Returns:
            The resolved relation data

        Raises:
            ValidationError: If parameter validation fails
            ResourceNotFoundError: If the entity or relation doesn't exist
            APIError: For other API errors
        """
        # Extract entity ID from object or use string directly
        if isinstance(entity, Entity):
            entity_id = entity.fb_entity_id
        else:
            entity_id = str(entity)

        # Get relation ID
        relation_id = self.relation_id

        # Merge parameters from dict and kwargs
        all_params = {}
        if parameters:
            all_params.update(parameters)
        if kwargs:
            all_params.update(kwargs)

        # Validate parameters if we have parameter definitions
        validated_params = {}
        if all_params and self.resolve_config and self.resolve_config.parameter_definition:
            validated_params = self.resolve_config.validate_parameters(
                all_params)

        # Use the appropriate client method based on what's available
        if hasattr(self, "_client") and self._client:
            client = self._client
        elif hasattr(Entity, "_current_client") and Entity._current_client:
            client = Entity._current_client
        else:
            from fusionbase.core.context import get_current_client
            client = get_current_client()
            if client is None:
                raise APIError(
                    "No client available. Please provide a client or use with statement."
                )

        # Always use POST, not GET - even for empty params
        data = client.request(
            "POST",
            f"relation/resolve/{relation_id}/{entity_id}",
            json=validated_params or {}  # Send empty dict if no parameters
        )

        return data

    async def aresolve(self,
                       entity: Union[Entity, str],
                       parameters: Optional[Dict[str, Any]] = None,
                       **kwargs) -> Any:
        """Asynchronously resolve this relation with the given entity.

        Args:
            entity: An entity instance or entity ID to resolve the relation with
            parameters: Optional parameters required by the relation as a dictionary
            **kwargs: Additional parameters as keyword arguments

        Returns:
            The resolved relation data

        Raises:
            ValidationError: If parameter validation fails
            ResourceNotFoundError: If the entity or relation doesn't exist
            APIError: For other API errors
        """
        # Extract entity ID from object or use string directly
        if isinstance(entity, Entity):
            entity_id = entity.fb_entity_id
        else:
            entity_id = str(entity)

        # Get relation ID
        relation_id = self.relation_id

        # Merge parameters from dict and kwargs
        all_params = {}
        if parameters:
            all_params.update(parameters)
        if kwargs:
            all_params.update(kwargs)

        # Validate parameters if we have parameter definitions
        validated_params = {}
        if all_params and self.resolve_config and self.resolve_config.parameter_definition:
            validated_params = self.resolve_config.validate_parameters(
                all_params)

        # Use the appropriate client method based on what's available
        client = None
        if hasattr(self, "_client") and self._client:
            client = self._client
        elif hasattr(Entity, "_current_client") and Entity._current_client:
            client = Entity._current_client
        else:
            from fusionbase.core.context import get_current_client
            client = get_current_client()
            if client is None:
                raise APIError(
                    "No client available. Please provide a client or use with statement."
                )

        # Try different async methods depending on client capabilities
        if hasattr(client, "arequest"):
            # Always use POST, not GET - even for empty params
            data = await client.arequest(
                "POST",
                f"relation/resolve/{relation_id}/{entity_id}",
                json=validated_params or {}  # Send empty dict if no parameters
            )
        elif hasattr(client, "aget"):
            # Use apost with the correct method
            data = await client.apost(
                f"relation/resolve/{relation_id}/{entity_id}",
                json=validated_params or {})
        elif hasattr(client, "_async_http_client"):
            # Direct use of async HTTP client
            response = await client._async_http_client.post(
                f"relation/resolve/{relation_id}/{entity_id}",
                json=validated_params or {})
            response.raise_for_status()
            data = response.json()
        else:
            # Fall back to sync method through asyncio.to_thread
            if hasattr(client, "request") and not inspect.iscoroutinefunction(
                    client.request):
                return await asyncio.to_thread(self.resolve, entity, parameters,
                                               **kwargs)

            # No suitable async method found, raise an error
            raise APIError(
                f"No suitable async method found to resolve relation (ID: {relation_id})",
                status_code=500)

        return data
