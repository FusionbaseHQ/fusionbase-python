from enum import Enum


class AddressComponentType(str, Enum):
    COUNTRY = "country"
    POSTAL_CODE = "postal_code"
    CITY = "city"
    STREET = "street"
    STATE = 'state'
    HOUSE_NUMBER = "house_number"
    COUNTY = "county"