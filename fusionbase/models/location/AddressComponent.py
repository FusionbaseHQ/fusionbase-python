from pydantic import BaseModel
# from typing import Dict

from fusionbase.models.location.AddressComponentType import AddressComponentType


class AddressComponent(BaseModel):
    component_type: AddressComponentType
    component_value: str