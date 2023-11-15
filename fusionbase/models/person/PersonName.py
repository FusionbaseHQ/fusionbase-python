from pydantic import BaseModel
from pydantic import Field

from typing import List


class PersonName(BaseModel):
	given: str
	family: str
	maiden: str | None = None
	aliases: List[str] = Field(default_factory=list)