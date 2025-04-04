"""Type definitions for Fusionbase entities."""

from enum import Enum


class EntityType(str, Enum):
    """Types of entities in the Fusionbase system."""

    ORGANIZATION = "organization"
    PERSON = "person"
    LOCATION = "location"
    EVENT = "event"


class PersonSubtype(str, Enum):
    """Subtypes of person entities."""

    INDIVIDUAL = "INDIVIDUAL"
    ANY = "ANY"


class OrganizationSubtype(str, Enum):
    """Subtypes of organization entities."""

    CORPORATION = "CORPORATION"
    ANY = "ANY"


class OrganizationStatus(str, Enum):
    """Status of an organization."""

    UNKNOWN = "UNKNOWN"
    INACTIVE = "INACTIVE"
    ACTIVE = "ACTIVE"
    LIQUIDATED = "LIQUIDATED"
    DISSOLVED = "DISSOLVED"
    LIQUIDATION = "LIQUIDATION"
    RECEIVER_ACTION = "RECEIVER_ACTION"
    CONVERTED_CLOSED = "CONVERTED_CLOSED"
    VOLUNTARY_ARRANGEMENT = "VOLUNTARY_ARRANGEMENT"
    INSOLVENCY_PROCEEDINGS = "INSOLVENCY_PROCEEDINGS"
    IN_ADMINISTRATION = "IN_ADMINISTRATION"
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    REGISTERED = "REGISTERED"
    OPERATIONAL = "OPERATIONAL"
    CLOSED_TEMPORARILY = "CLOSED_TEMPORARILY"
    CLOSED_PERMANENTLY = "CLOSED_PERMANENTLY"


class OrganizationStatusDetail(str, Enum):
    """Detailed status information for organizations."""

    ACTIVE_PROPOSAL_TO_STRIKE_OFF = "ACTIVE_PROPOSAL_TO_STRIKE_OFF"
    CONVERTED_TO_PLC = "CONVERTED_TO_PLC"
    CONVERTED_TO_UK_SOCIETAS = "CONVERTED_TO_UK_SOCIETAS"
    CONVERTED_TO_UKEIG = "CONVERTED_TO_UKEIG"
    TRANSFER_FROM_UK = "TRANSFER_FROM_UK"
    TRANSFORMED_TO_SE = "TRANSFORMED_TO_SE"


class LocationSubtype(str, Enum):
    """Subtypes of location entities."""

    POINT_OF_INTEREST = "POINT_OF_INTEREST"
    LOCALITY = "LOCALITY"
    STREET = "STREET"
    POSTAL_CODE = "POSTAL_CODE"
    CITY_POSTAL_CODE = "CITY_POSTAL_CODE"
    CITY_NO_POSTAL_CODE = "CITY_NO_POSTAL_CODE"
    ADMINISTRATIVE_AREA = "ADMINISTRATIVE_AREA"
    COUNTY = "COUNTY"
    STATE = "STATE"
    COUNTRY = "COUNTRY"
    ANY = "ANY"


class AddressComponentType(str, Enum):
    """Types of address components."""

    COUNTRY = "country"
    POSTAL_CODE = "postal_code"
    CITY = "city"
    STREET = "street"
    STATE = "state"
    HOUSE_NUMBER = "house_number"
    COUNTY = "county"
