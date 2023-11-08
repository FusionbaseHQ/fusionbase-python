from pydantic import BaseModel
from typing import Optional
import httpx

class Entity(BaseModel):
    client: Optional[httpx.Client] = None
    
    class Config:
        arbitrary_types_allowed = True