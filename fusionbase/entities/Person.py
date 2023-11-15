import logging
import os

from pydantic import BaseModel
from pydantic import field_validator
from pydantic import Field
from pydantic import model_validator
from pydantic import computed_field

from typing import ClassVar
from typing import Any
from typing import Dict


from datetime import date


from fusionbase.entities.Entity import Entity
from fusionbase.entities.Location import Location
from fusionbase.models.person.PersonName import PersonName


class Person(Entity):

    entity_type: str = Field(default="PERSON")
    entity_subtype: str = Field(default="INDIVIDUAL")

    # fb_semantic_id: str | None = None

    name: PersonName | Dict[str, str] | None = None
    locations: Dict[str, Any] | None = None
    birth_date: date | None = None
    source: Dict[str, str] | None = None

    @field_validator('name', mode='before')
    @classmethod
    def validate_name(cls, val):
        if isinstance(val, PersonName):
            return val

        person_name = PersonName(**val)
        return person_name

    @field_validator('birth_date', mode='before')
    @classmethod
    def validate_birth_date(cls, value):
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise ValueError(f"Invalid date format for birth_date: {value}")

    @field_validator('locations', mode='before')
    @classmethod
    def validate_locations(cls, value):
        if isinstance(value, dict):
            for k, v in value.items():
                if isinstance(v, dict):
                    value[k] = Location(**v)
                elif isinstance(v, Location):
                    pass
                elif v is None:
                    pass
                else:
                    raise ValueError(f'Wrong locations format')

        return value