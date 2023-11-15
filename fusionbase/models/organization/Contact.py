from typing import Union
from pydantic import BaseModel, constr, HttpUrl, field_validator, model_serializer, ConfigDict
from pydantic import ValidationError


class Websites(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)
    primary: str | None
    
    @field_validator('primary')
    def stringify_url(cls, v):
        if v is None:
            return v

        try:
            url = HttpUrl(v)
            return str(url)
        except ValidationError:
            return None


class PhoneNumbers(BaseModel):
    primary: Union[constr(strip_whitespace=True, min_length=1), None]


class Contact(BaseModel):
    websites: Websites
    phone_numbers: PhoneNumbers
