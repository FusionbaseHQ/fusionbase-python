from pydantic import BaseModel, constr, HttpUrl, validator
from typing import List
from typing import Dict


class Value(BaseModel):
    de: constr(strip_whitespace=True, min_length=1)


class Web(BaseModel):
    source: str
    value: Value


class Classifications(BaseModel):
    web: List[Web] | None = None
    industry_codes: List[Dict[str, str]] | None = None
    legal_form: Dict[str, str] | None = None
