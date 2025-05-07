"""Event entity module."""

from datetime import datetime
from typing import Any, ClassVar, Dict, Optional, Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from fusionbase.data.source import Source
from fusionbase.entities.base import Entity
from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person
from fusionbase.types.entities import ConflictEventCategory
from fusionbase.types.entities import CyberSecurityEventCategory
from fusionbase.types.entities import EntityType
from fusionbase.types.entities import EventStatus
from fusionbase.types.entities import EventSubtype
from fusionbase.types.entities import NaturalEventCategory
from fusionbase.types.entities import PublicationEventCategory


class LocalizedText(BaseModel):
    """Text with language codes."""

    model_config = ConfigDict(extra="allow")
    en: Optional[str] = None
    de: Optional[str] = None


class ShortDescription(BaseModel):
    """Short description in multiple languages."""

    model_config = ConfigDict(extra="allow")
    short: Optional[LocalizedText] = None
    long: Optional[LocalizedText] = None


class LinkedPerson(BaseModel):
    """Reference to a Person entity."""

    fb_entity_id: str
    name: Optional[Dict[str, Any]] = None
    _person_ref: Optional[LazyReference[Person]] = None

    @property
    def person(self) -> Optional[Person]:
        """Get the referenced Person entity."""
        if self._person_ref:
            return self._person_ref.get()
        return None

    async def aget_person(self) -> Optional[Person]:
        """Get the referenced Person entity asynchronously."""
        if self._person_ref:
            return await self._person_ref.aget()
        return None


class LinkedOrganization(BaseModel):
    """Reference to an Organization entity."""

    fb_entity_id: str
    name: Optional[str] = None
    _organization_ref: Optional[LazyReference[Organization]] = None

    @property
    def organization(self) -> Optional[Organization]:
        """Get the referenced Organization entity."""
        if self._organization_ref:
            return self._organization_ref.get()
        return None

    async def aget_organization(self) -> Optional[Organization]:
        """Get the referenced Organization entity asynchronously."""
        if self._organization_ref:
            return await self._organization_ref.aget()
        return None


class LinkedEntities(BaseModel):
    """Container for linked entities."""

    person: Optional[LinkedPerson] = None
    organization: Optional[LinkedOrganization] = None


class RoleInfo(BaseModel):
    """Information about a role within an event."""

    name: Optional[str] = None
    original_name_source: Optional[str] = None
    responsibilities: Optional[str] = None
    representation_scheme: Optional[str] = None
    liability_deposit: Optional[str] = None


class EventValue(BaseModel):
    """Value details for an event."""

    role: Optional[RoleInfo] = None


class EventDetails(BaseModel):
    """Detailed information about an event."""

    value: Optional[EventValue] = None
    linked_entities: Optional[LinkedEntities] = None


class Event(Entity):
    """Event entity in Fusionbase.

    Events represent significant occurrences related to organizations, persons,
    or locations, such as corporate milestones, publications, or natural events.

    Attributes:
        name: The name of the event in different languages
        description: Descriptions of the event in different languages
        start_date: When the event started
        end_date: When the event ended
        live_date: When the event went live
        announce_date: When the event was announced
        status: Current status of the event (e.g., FINISHED)
        category: Category of the event (e.g., MEMBER_EXIT_POSITION)
        source: Information about the data source
        details: Detailed information including linked entities
        origin_location: Where the event originated
        event_location: Where the event took place
        effect_location: Where the event had an effect
    """

    # Direct API response fields
    name: Optional[LocalizedText] = None
    description: Optional[ShortDescription] = None
    start_date: Optional[Union[datetime, str]] = None
    live_date: Optional[Union[datetime, str]] = None
    end_date: Optional[Union[datetime, str]] = None
    announce_date: Optional[Union[datetime, str]] = None
    status: Optional[EventStatus] = None
    category: Optional[Union[PublicationEventCategory, NaturalEventCategory,
                             CyberSecurityEventCategory, ConflictEventCategory,
                             str]] = None
    source: Optional[Source] = None
    details: Optional[EventDetails] = None
    origin_location: Optional[Union[Location, Dict[str, Any]]] = None
    event_location: Optional[Union[Location, Dict[str, Any]]] = None
    effect_location: Optional[Union[Location, Dict[str, Any]]] = None

    # Entity type information
    entity_type: ClassVar[EntityType] = EntityType.EVENT
    entity_subtype: EventSubtype = EventSubtype.ANY

    # Default for fb_entity_version to handle search results
    fb_entity_version: str = ""

    @model_validator(mode='before')
    @classmethod
    def set_defaults_and_process_entities(cls, data):
        """Process linked entities and set defaults."""
        if not isinstance(data, dict):
            return data

        # Set default fb_entity_version if missing
        if 'fb_entity_version' not in data or data['fb_entity_version'] is None:
            data['fb_entity_version'] = ""

        # Process the linked locations
        for location_field in [
                'origin_location', 'event_location', 'effect_location'
        ]:
            if location_field in data and isinstance(
                    data[location_field],
                    dict) and 'fb_entity_id' in data[location_field]:
                try:
                    # Convert to Location object
                    location_data = data[location_field]
                    data[location_field] = Location.model_validate(
                        location_data)
                except Exception:
                    # If validation fails, keep the original data
                    pass

        return data

    def _process_linked_entities(self, client):
        """Process linked entities after initialization.

        This creates LazyReference objects for related entities.
        """
        if not self.details or not self.details.linked_entities:
            return

        linked_entities = self.details.linked_entities

        # Process linked person
        if linked_entities.person and linked_entities.person.fb_entity_id:
            linked_entities.person._person_ref = LazyReference(
                linked_entities.person.fb_entity_id, Person, client)

        # Process linked organization
        if linked_entities.organization and linked_entities.organization.fb_entity_id:
            linked_entities.organization._organization_ref = LazyReference(
                linked_entities.organization.fb_entity_id, Organization, client)

    @property
    def linked_person(self) -> Optional[Person]:
        """Get the linked Person entity if available."""
        if (self.details and self.details.linked_entities and
                self.details.linked_entities.person):
            return self.details.linked_entities.person.person
        return None

    @property
    def linked_organization(self) -> Optional[Organization]:
        """Get the linked Organization entity if available."""
        if (self.details and self.details.linked_entities and
                self.details.linked_entities.organization):
            return self.details.linked_entities.organization.organization
        return None

    async def aget_linked_person(self) -> Optional[Person]:
        """Get the linked Person entity asynchronously if available."""
        if (self.details and self.details.linked_entities and
                self.details.linked_entities.person):
            return await self.details.linked_entities.person.aget_person()
        return None

    async def aget_linked_organization(self) -> Optional[Organization]:
        """Get the linked Organization entity asynchronously if available."""
        if (self.details and self.details.linked_entities and
                self.details.linked_entities.organization):
            return await self.details.linked_entities.organization.aget_organization(
            )
        return None

    @property
    def event_title(self) -> Optional[str]:
        """Get the event title in English or German."""
        if not self.name:
            return None
        if self.name.en:
            return self.name.en
        if self.name.de:
            return self.name.de
        return None

    @property
    def event_description(self) -> Optional[str]:
        """Get the short description in English or German."""
        if not self.description or not self.description.short:
            return None
        if self.description.short.en:
            return self.description.short.en
        if self.description.short.de:
            return self.description.short.de
        return None

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Event":
        """Internal method to create an Event instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request
        data = make_entity_request(client, cls.entity_type.value, entity_id)

        # Create the event instance
        event = cls.model_validate(data)

        # Process linked entities to create LazyReference objects
        event._process_linked_entities(client)

        return event

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Event":
        """Asynchronously create an Event instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request_async
        data = await make_entity_request_async(client, cls.entity_type.value,
                                               entity_id)

        # Create the event instance
        event = cls.model_validate(data)

        # Process linked entities to create LazyReference objects
        event._process_linked_entities(client)

        return event
