from pydantic import BaseModel
from typing import Optional
import httpx

class Entity(BaseModel):
    client: Optional[httpx.Client] = None
    fb_entity_id: str
    
    class Config:
        arbitrary_types_allowed = True