"""Organization entity module."""

import asyncio
from datetime import date
from datetime import datetime
import inspect
from typing import ClassVar, Dict, List, Optional, Union

import httpx
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from fusionbase.entities.base import Entity
from fusionbase.entities.location import Location
from fusionbase.exceptions import APIError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import ResourceNotFoundError
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
    def _from_id(cls, client, entity_id: str) -> "Organization":  # pylint: disable=too-many-branches
        """Internal method to create an Organization instance by fetching it from the API.

        Args:
            client: The Fusionbase client
            entity_id: ID of the organization to fetch

        Returns:
            An Organization instance with fully resolved Location objects

        Raises:
            ResourceNotFoundError: If the organization doesn't exist
            AuthenticationError: If authentication fails
            AuthorizationError: If the user is not authorized
            APIError: For other API errors
        """
        # Use the request method with retry if available
        if hasattr(client, "request"):
            try:
                data = client.request("GET",
                                      f"entities/organization/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to organizations
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("organization", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("organization", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        elif hasattr(client, "make_request"):
            # If client is an EntityManager
            try:
                data = client.make_request(
                    f"entities/organization/get/{entity_id}")
            except ResourceNotFoundError as e:
                # Make the error more specific to organizations
                response = getattr(e, "response", None)
                raise ResourceNotFoundError("organization", entity_id,
                                            response) from e
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("organization", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:
                raise e
        else:
            try:
                response = client.http_client.get(
                    f"entities/organization/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError as e:
                # Use our error parser to generate appropriate exceptions
                if e.response.status_code == 404:
                    raise ResourceNotFoundError("organization", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            except Exception as e:  # pylint: disable=broad-except
                if getattr(e, "response", None) is not None:
                    raise parse_error_response(getattr(e, "response")) from e

                raise APIError(
                    f"Failed to retrieve organization (ID: {entity_id}): {e}",
                    500,
                ) from e

        # Return the model_validate result directly
        return cls.model_validate(data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Organization":
        """Create Organization instance by async fetching."""
        try:
            data = None
            # Try direct async methods on the client
            if hasattr(client, "aget"):
                data = await client.aget(
                    f"entities/organization/get/{entity_id}")
            # Use arequest method if available
            elif hasattr(client, "arequest"):
                data = await client.arequest(
                    "GET", f"entities/organization/get/{entity_id}")
            # Use amake_request method if available (for entity managers)
            elif hasattr(client, "amake_request"):
                data = await client.amake_request(
                    f"entities/organization/get/{entity_id}")
            # Use async HTTP client directly
            elif hasattr(client, "_async_http_client"):
                response = await client._async_http_client.get(
                    f"entities/organization/get/{entity_id}")
                response.raise_for_status()
                data = response.json()
            else:
                # Fall back to sync method through asyncio.to_thread
                if hasattr(client,
                           "request") and not inspect.iscoroutinefunction(
                               client.request):
                    data = await asyncio.to_thread(cls._from_id, client,
                                                   entity_id)
                    return data  # Return early as we already have an Organization instance

                # No suitable async method found, raise an error
                raise APIError(
                    f"No suitable async method found to fetch organization (ID: {entity_id})",
                    status_code=500)

            # Make sure we have data
            if not data:
                raise APIError(
                    f"Failed to retrieve organization data (ID: {entity_id})",
                    status_code=500)

            # Return the model_validate result directly
            return cls.model_validate(data)

        except ResourceNotFoundError as e:
            # Make the error more specific to organizations
            response = getattr(e, "response", None)
            raise ResourceNotFoundError("organization", entity_id,
                                        response) from e
        except httpx.HTTPStatusError as e:
            if getattr(e, "response", None) is not None:
                if getattr(e.response, "status_code", None) == 404:
                    raise ResourceNotFoundError("organization", entity_id,
                                                e.response) from e
                raise parse_error_response(e.response) from e
            raise APIError(("Failed to retrieve organization "
                            f"(ID: {entity_id}): {e}"), 500) from e
