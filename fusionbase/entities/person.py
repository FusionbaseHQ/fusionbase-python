"""Person entity module."""

import asyncio
from datetime import datetime
import inspect
from typing import ClassVar, Dict, List, Optional

import httpx
from pydantic import BaseModel
from pydantic import model_validator

from fusionbase.entities.base import Entity
from fusionbase.entities.location import Location
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.types.entities import EntityType
from fusionbase.types.entities import PersonSubtype


class PersonName(BaseModel):
    """Person name components."""

    given: Optional[str] = None
    family: Optional[str] = None
    maiden: Optional[str] = None
    aliases: List[str] = []


class BirthDate(BaseModel):
    """Person birth date information."""

    value: Optional[datetime] = None
    is_month: bool = False


class Source(BaseModel):
    """Source information.

    Contains information about the data source for the person entity.

    Attributes:
        id: Source identifier
    """

    id: Optional[str] = None


class Person(Entity):
    """Person entity in Fusionbase.

    Attributes:
        name: The person's name components
        locations: Dictionary of locations associated with the person
        birth_date: The person's birth date information
        source: Source of the person data
        entity_subtype: Type of person (e.g., INDIVIDUAL)
    """

    # Direct API response fields
    name: PersonName = PersonName()
    locations: Dict[str,
                    Location] = {}  # Always fully resolved Location objects
    birth_date: Optional[BirthDate] = None
    source: Optional[Source] = None

    # Entity type information
    entity_type: ClassVar[EntityType] = EntityType.PERSON
    entity_subtype: PersonSubtype = PersonSubtype.ANY

    # Add default for fb_entity_version to handle search results
    fb_entity_version: str = ""

    @model_validator(mode='before')
    @classmethod
    def set_defaults(cls, data):
        """Set default values for missing fields in search results."""
        if isinstance(data, dict):
            # Set a default fb_entity_version if missing
            if 'fb_entity_version' not in data or data[
                    'fb_entity_version'] is None:
                data['fb_entity_version'] = ""

            # Process locations data if present
            if 'locations' in data and isinstance(data['locations'], dict):
                locations = {}
                for key, loc_data in data['locations'].items():
                    # Skip None values to avoid validation errors
                    if loc_data is None:
                        continue

                    if isinstance(loc_data,
                                  dict) and 'fb_entity_id' in loc_data:
                        # Convert location data directly to a Location object
                        try:
                            # Ensure we have a properly formatted location object
                            location_data = {
                                "fb_entity_id":
                                    loc_data["fb_entity_id"],
                                "fb_entity_version":
                                    loc_data.get("fb_entity_version", ""),
                                "name":
                                    loc_data.get("formatted_address"),
                                "metadata": {
                                    "created_at": loc_data.get("created_at"),
                                    "updated_at": loc_data.get("updated_at"),
                                    "fb_datetime": loc_data.get("fb_datetime"),
                                },
                                "external_ids":
                                    loc_data.get("external_ids", {}),
                                "coordinate":
                                    loc_data.get("coordinate"),
                                "location_level":
                                    loc_data.get("location_level"),
                                "address_components":
                                    loc_data.get("address_components", []),
                                "alternative_names":
                                    loc_data.get("alternative_names", []),
                                "formatted_address":
                                    loc_data.get("formatted_address"),
                            }
                            locations[key] = Location.model_validate(
                                location_data)
                        except Exception:
                            # If validation fails, keep the original data
                            locations[key] = loc_data
                    else:
                        locations[key] = loc_data
                data['locations'] = locations

        return data

    @property
    def given_name(self) -> Optional[str]:
        """Get the person's given name."""
        return self.name.given if self.name else None

    @property
    def family_name(self) -> Optional[str]:
        """Get the person's family name."""
        return self.name.family if self.name else None

    @property
    def home_location(self) -> Optional[Location]:
        """Get the person's home location.

        Returns:
            A Location object for the home location if available
        """
        return self.locations.get("home")

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Person":  # pylint: disable=too-many-branches
        """Internal method to create a Person instance by fetching it from the API.

        Args:
            client: The Fusionbase client
            entity_id: ID of the person to fetch

        Returns:
            A Person instance with fully resolved Location objects

        Raises:
            ResourceNotFoundError: If the person doesn't exist
            AuthenticationError: If authentication fails
            AuthorizationError: If the user is not authorized
            APIError: For other API errors
        """
        # Use the request method with retry if available
        if hasattr(client, "request"):
            try:
                data = client.request("GET", f"entities/person/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to persons
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("person", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("person", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        elif hasattr(client, "make_request"):
            # If client is an EntityManager
            try:
                data = client.make_request(f"entities/person/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to persons
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("person", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("person", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        else:
            try:
                response = client.http_client.get(
                    f"entities/person/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError as e:
                # Use our error parser to generate appropriate exceptions
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("person", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:  # pylint: disable=broad-except
                if getattr(e, "response", None) is not None:
                    raise parse_error_response(getattr(e, "response")) from e

                raise APIError(
                    f"Failed to retrieve person (ID: {entity_id}): {e}",
                    500,
                ) from e

        # No need to process locations separately since the model_validator will handle it
        # Just return the model_validate result directly
        return cls.model_validate(data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Person":
        """Create Person instance by async fetching."""
        try:
            data = None
            # Try direct async methods on the client
            if hasattr(client, "aget"):
                data = await client.aget(f"entities/person/get/{entity_id}")
            # Use arequest method if available
            elif hasattr(client, "arequest"):
                data = await client.arequest(
                    "GET", f"entities/person/get/{entity_id}")
            # Use amake_request method if available (for entity managers)
            elif hasattr(client, "amake_request"):
                data = await client.amake_request(
                    f"entities/person/get/{entity_id}")
            # Use async HTTP client directly
            elif hasattr(client, "_async_http_client"):
                response = await client._async_http_client.get(
                    f"entities/person/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            else:
                # Fall back to sync method through asyncio.to_thread
                if hasattr(client,
                           "request") and not inspect.iscoroutinefunction(
                               client.request):
                    data = await asyncio.to_thread(cls._from_id, client,
                                                   entity_id)
                    return data  # Return early as we already have a Person instance

                # No suitable async method found, raise an error
                raise APIError(
                    f"No suitable async method found to fetch person (ID: {entity_id})",
                    status_code=500)

            # Make sure we have data
            if not data:
                raise APIError(
                    f"Failed to retrieve person data (ID: {entity_id})",
                    status_code=500)

            # No need to process locations separately since the model_validator will handle it
            # Just return the model_validate result directly
            return cls.model_validate(data)

        except ResourceNotFoundError as e:
            # Make the error more specific to persons
            response = getattr(e, "response", None)
            raise ResourceNotFoundError("person", entity_id, response) from e
        except httpx.HTTPStatusError as e:
            if getattr(e, "response", None) is not None:
                if getattr(e.response, "status_code", None) == 404:
                    raise ResourceNotFoundError("person", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            raise APIError(("Failed to retrieve person "
                            f"(ID: {entity_id}): {e}"), 500) from e
