"""Organization entity module."""

from datetime import date
from datetime import datetime
from typing import ClassVar, Dict, List, Optional, Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from fusionbase.entities.base import Entity
from fusionbase.entities.location import Location
from fusionbase.types.entities import EntityType
from fusionbase.types.entities import OrganizationStatus
from fusionbase.types.entities import OrganizationStatusDetail
from fusionbase.types.entities import OrganizationSubtype


class OrganizationState(BaseModel):
    """Organization state information."""

    active: bool
    status: OrganizationStatus
    status_detail: Optional[OrganizationStatusDetail] = None


class Websites(BaseModel):
    """Organization website information."""

    primary: Optional[str] = None


class PhoneNumbers(BaseModel):
    """Organization phone number information."""

    primary: Optional[str] = None


class DynamicEmails(BaseModel):
    """Organization email addresses."""

    model_config = ConfigDict(extra="allow")
    primary: Optional[str] = None


class Contact(BaseModel):
    """Organization contact information."""

    websites: Optional[Websites] = None
    phone_numbers: Optional[PhoneNumbers] = None
    emails: Optional[DynamicEmails] = None


class Value(BaseModel):
    """Value with language code."""

    de: str


class Web(BaseModel):
    """Web classification."""

    source: str
    value: Value


class IndustryClassification(BaseModel):
    """Industry classification."""

    system: str
    code: Optional[str] = None
    label: Optional[Value] = None
    description: Optional[Value] = None


class Classifications(BaseModel):
    """Organization classifications."""

    web: Optional[List[Web]] = None
    industry_classifications: Optional[List[IndustryClassification]] = None
    legal_form: Optional[dict] = None


class Jurisdiction(BaseModel):
    """Organization jurisdiction."""

    iso_alpha3: Optional[str] = None


class RegistrationAuthority(BaseModel):
    """Registration authority information."""

    registration_authority_id: Optional[str] = None
    registration_authority_name: Optional[str] = None
    registration_authority_entity_id: Optional[str] = None
    registration_authority_entity_name: Optional[str] = None
    registration_type: Optional[str] = None
    registration_number: Optional[str] = None
    registration_id_extra: Optional[str] = None
    registration_authority_location: Optional[Location] = None


class RegistrationData(BaseModel):
    """Legal registration data."""

    registration_authority: Optional[Dict[str, RegistrationAuthority]] = None


class Organization(Entity):
    """Organization entity in Fusionbase.

    Attributes:
        status: Current state of the organization
        name: Official name of the organization
        other_names: Alternative names or trading names
        description: Description of the organization's activities
        address: Main address of the organization
        other_addresses: Additional addresses
        jurisdiction: Legal jurisdiction
        contact: Contact information including websites and phone numbers
        founding_date: Date when the organization was founded
        cessation_date: Date when the organization ceased operations
        classifications: Various classifications including industry codes
        legal: Legal registration details
        entity_subtype: Type of organization (e.g., CORPORATION)
    """

    # Direct API response fields
    status: OrganizationState = OrganizationState(
        active=True, status=OrganizationStatus.UNKNOWN)
    name: Optional[str] = None
    other_names: Optional[List[str]] = None
    description: Optional[Dict[str, dict]] = None
    address: Optional[Location] = None
    other_addresses: Optional[List[Location]] = None
    jurisdiction: Optional[Jurisdiction] = None
    contact: Optional[Contact] = None
    founding_date: Optional[Union[datetime, date]] = None
    cessation_date: Optional[Union[datetime, date]] = None
    classifications: Optional[Classifications] = None
    legal: Optional[RegistrationData] = None
    source: Optional[Dict[str, str]] = None

    # Entity type information
    entity_type: ClassVar[EntityType] = EntityType.ORGANIZATION
    entity_subtype: OrganizationSubtype = OrganizationSubtype.CORPORATION

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

            # Process address data if present
            if 'address' in data and isinstance(
                    data['address'],
                    dict) and 'fb_entity_id' in data['address']:
                try:
                    # Convert address to Location object
                    location_data = data['address']
                    data['address'] = Location.model_validate(location_data)
                except Exception:
                    # If validation fails, keep the original data
                    pass

            # Process registration_authority_location if present
            if 'legal' in data and isinstance(
                    data['legal'],
                    dict) and 'registration_authority' in data['legal']:
                reg_authority = data['legal']['registration_authority']
                if isinstance(reg_authority, dict) and 'local' in reg_authority:
                    local_auth = reg_authority['local']
                    if isinstance(local_auth, dict) and 'registration_authority_location' in local_auth and \
                       isinstance(local_auth['registration_authority_location'], dict):
                        try:
                            # Convert to Location object
                            location_data = local_auth[
                                'registration_authority_location']
                            local_auth[
                                'registration_authority_location'] = Location.model_validate(
                                    location_data)
                        except Exception:
                            # If validation fails, keep the original data
                            pass

        return data

    @property
    def primary_website(self) -> Optional[str]:
        """Get the primary website URL."""
        if self.contact and self.contact.websites:
            return self.contact.websites.primary
        return None

    @property
    def primary_phone(self) -> Optional[str]:
        """Get the primary phone number."""
        if self.contact and self.contact.phone_numbers:
            return self.contact.phone_numbers.primary
        return None

    @property
    def is_active(self) -> bool:
        """Check if the organization is active."""
        return self.status.active if self.status else False

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Organization":
        """Internal method to create an Organization instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request
        data = make_entity_request(client, cls.entity_type.value, entity_id)
        return cls.model_validate(data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Organization":
        """Asynchronously create an Organization instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request_async
        data = await make_entity_request_async(client, cls.entity_type.value,
                                               entity_id)
        return cls.model_validate(data)
