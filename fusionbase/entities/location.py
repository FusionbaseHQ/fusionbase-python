"""Location entity module."""

import asyncio
import inspect  # Added missing import
from typing import ClassVar, List, Optional

import httpx
from pydantic import BaseModel
from pydantic import model_validator

from fusionbase.entities.base import Entity
from fusionbase.entities.types import AddressComponentType
from fusionbase.entities.types import EntityType
from fusionbase.entities.types import LocationSubtype
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import ResourceNotFoundError


class Coordinate(BaseModel):
    """Geographical coordinates."""

    latitude: float
    longitude: float


class AddressComponent(BaseModel):
    """Address component with type and value."""

    component_type: AddressComponentType
    component_value: str


class Location(Entity):
    """Location entity in Fusionbase.

    Attributes:
        coordinate: Geographical coordinates
        location_level: Hierarchical level of the location
        address_components: Detailed address components
        alternative_names: Alternative names for the location
        fb_semantic_id: Semantic identifier for the location
        formatted_address: Human-readable address format
        entity_subtype: Type of location (e.g., CITY, STREET, etc.)
    """

    # Direct API response fields
    coordinate: Optional[Coordinate] = None
    location_level: Optional[str] = None
    address_components: List[AddressComponent] = []
    alternative_names: List[str] = []
    fb_semantic_id: Optional[str] = None
    formatted_address: Optional[str] = None

    # Entity type information
    entity_type: ClassVar[EntityType] = EntityType.LOCATION
    entity_subtype: LocationSubtype = LocationSubtype.ANY

    @model_validator(mode='before')
    @classmethod
    def process_location_data(cls, data):
        """Set default values and process fields for locations."""
        if isinstance(data, dict):
            # Set default fb_entity_version if missing
            if 'fb_entity_version' not in data or data[
                    'fb_entity_version'] is None:
                data['fb_entity_version'] = ""

            # Make sure address_components is a list
            if 'address_components' not in data or data[
                    'address_components'] is None:
                data['address_components'] = []

            # Make sure alternative_names is a list
            if 'alternative_names' not in data or data[
                    'alternative_names'] is None:
                data['alternative_names'] = []

            # Handle coordinate validation issues
            if 'coordinate' in data and data['coordinate'] is not None:
                if not isinstance(data['coordinate'], dict) or \
                   'latitude' not in data['coordinate'] or \
                   'longitude' not in data['coordinate']:
                    # Invalid coordinate data
                    data['coordinate'] = None

            # Fix invalid locations serialization issues - fix name field if missing
            if 'name' not in data or data['name'] is None:
                data['name'] = data.get('formatted_address', '')

        return data

    @property
    def address(self) -> Optional[str]:
        """Get the main address string."""
        return self.formatted_address

    @property
    def city(self) -> Optional[str]:
        """Get the city name from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.CITY:
                return component.component_value
        return None

    @property
    def country(self) -> Optional[str]:
        """Get the country name from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.COUNTRY:
                return component.component_value
        return None

    @property
    def postal_code(self) -> Optional[str]:
        """Get the postal code from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.POSTAL_CODE:
                return component.component_value
        return None

    @property
    def state(self) -> Optional[str]:
        """Get the state name from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.STATE:
                return component.component_value
        return None

    @property
    def county(self) -> Optional[str]:
        """Get the county name from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.COUNTY:
                return component.component_value
        return None

    @property
    def street(self) -> Optional[str]:
        """Get the street name from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.STREET:
                return component.component_value
        return None

    @property
    def house_number(self) -> Optional[str]:
        """Get the house number from address components."""
        for component in self.address_components:
            if component.component_type == AddressComponentType.HOUSE_NUMBER:
                return component.component_value
        return None

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Location":  # pylint: disable=too-many-branches
        """Internal method to create a Location instance by fetching it from the API.

        Args:
            client: The Fusionbase client
            entity_id: ID of the location to fetch

        Returns:
            A Location instance

        Raises:
            ResourceNotFoundError: If the location doesn't exist
            AuthenticationError: If authentication fails
            AuthorizationError: If the user is not authorized
            APIError: For other API errors
        """
        # Use the request method with retry if available
        if hasattr(client, "request"):
            try:
                data = client.request("GET",
                                      f"entities/location/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to locations
                # Use getattr to safely access response attribute or default to None
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("location", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("location", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        elif hasattr(client, "make_request"):
            # If client is an EntityManager
            try:
                data = client.make_request(f"entities/location/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to locations
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("location", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("location", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        else:
            try:
                response = client.http_client.get(
                    f"entities/location/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError as e:
                # Use our error parser to generate appropriate exceptions
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("location", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:  # pylint: disable=broad-except
                if getattr(e, "response", None) is not None:
                    raise parse_error_response(getattr(e, "response")) from e

                raise APIError(
                    f"Failed to retrieve location (ID: {entity_id}): {e}",
                    500) from e

        # Convert the API response to match our entity model
        location_data = {
            "fb_entity_id": data["fb_entity_id"],
            "fb_entity_version": data["fb_entity_version"],
            "name": data.get("formatted_address"),
            "metadata": {
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
                "fb_datetime": data.get("fb_datetime"),
                "fb_semantic_id": data.get("fb_semantic_id"),
            },
            "external_ids": data.get("external_ids", {}),
            "coordinate": data.get("coordinate"),
            "location_level": data.get("location_level"),
            "address_components": data.get("address_components", []),
            "alternative_names": data.get("alternative_names", []),
            "fb_semantic_id": data.get("fb_semantic_id"),
            "formatted_address": data.get("formatted_address"),
            # Parse entity subtype as enum
            "entity_subtype": data.get("entity_subtype", LocationSubtype.ANY),
        }

        return cls.model_validate(location_data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Location":
        """Create Location instance by async fetching."""
        try:
            data = None
            # Use async client if available
            if hasattr(client, "async_client") and client.async_client:
                # Get async client
                async_client = client.async_client
                data = await async_client.request(
                    "GET", f"entities/location/get/{entity_id}")
            # Use arequest method if available
            elif hasattr(client, "arequest"):
                data = await client.arequest(
                    "GET", f"entities/location/get/{entity_id}")
            # Use amake_request method if available (for entity managers)
            elif hasattr(client, "amake_request"):
                data = await client.amake_request(
                    f"entities/location/get/{entity_id}")
            # Use async HTTP client directly
            elif hasattr(client, "_async_http_client"):
                response = await client._async_http_client.get(
                    f"entities/location/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            else:
                # Fall back to sync method through asyncio.to_thread
                # BUT only if the client's request method is not async
                if hasattr(client,
                           "request") and not inspect.iscoroutinefunction(
                               client.request):
                    data = await asyncio.to_thread(cls._from_id, client,
                                                   entity_id)
                    return data  # Return early as we already have a Location instance

                raise APIError(
                    f"No suitable async method found to fetch location (ID: {entity_id})",
                    status_code=500)

            # Make sure we have data
            if not data:
                raise APIError(
                    f"Failed to retrieve location data (ID: {entity_id})",
                    status_code=500)

            # Convert the API response to match our entity model (same as sync version)
            location_data = {
                "fb_entity_id":
                    data["fb_entity_id"],
                "fb_entity_version":
                    data["fb_entity_version"],
                "name":
                    data.get("formatted_address"),
                "metadata": {
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at"),
                    "fb_datetime": data.get("fb_datetime"),
                    "fb_semantic_id": data.get("fb_semantic_id"),
                },
                "external_ids":
                    data.get("external_ids", {}),
                "coordinate":
                    data.get("coordinate"),
                "location_level":
                    data.get("location_level"),
                "address_components":
                    data.get("address_components", []),
                "alternative_names":
                    data.get("alternative_names", []),
                "fb_semantic_id":
                    data.get("fb_semantic_id"),
                "formatted_address":
                    data.get("formatted_address"),
                "entity_subtype":
                    data.get("entity_subtype", LocationSubtype.ANY),
            }
            return cls.model_validate(location_data)

        except ResourceNotFoundError as e:
            # Make the error more specific to locations
            response = getattr(e, "response", None)
            raise ResourceNotFoundError("location", entity_id, response) from e
        except httpx.HTTPStatusError as e:
            if getattr(e, "response", None) is not None:
                if getattr(e.response, "status_code", None) == 404:
                    raise ResourceNotFoundError("location", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            raise APIError(("Failed to retrieve location "
                            f"(ID: {entity_id}): {e}"), 500) from e
