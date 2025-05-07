"""Person entity module."""

from datetime import datetime
from typing import ClassVar, Dict, List, Optional

from pydantic import BaseModel
from pydantic import model_validator

from fusionbase.data.source import Source
from fusionbase.entities.base import Entity
from fusionbase.entities.location import Location
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
    def _from_id(cls, client, entity_id: str) -> "Person":
        """Internal method to create a Person instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request
        data = make_entity_request(client, cls.entity_type.value, entity_id)

        # Use the existing model_validator to process locations
        return cls.model_validate(data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Person":
        """Asynchronously create a Person instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request_async
        data = await make_entity_request_async(client, cls.entity_type.value,
                                               entity_id)

        # Use the existing model_validator to process locations
        return cls.model_validate(data)
