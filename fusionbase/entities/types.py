"""Type definitions for Fusionbase entities."""

from enum import Enum


class EntityType(str, Enum):
    """Types of entities in the Fusionbase system."""

    ORGANIZATION = "organization"
    PERSON = "person"
    LOCATION = "location"
    EVENT = "event"


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
