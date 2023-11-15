from fusionbase.entities.Entity import Entity

from pydantic import BaseModel
import httpx

from typing import Dict
from typing import Any
from typing import Optional
from datetime import datetime
from datetime import date
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


class Relation(BaseModel):  
    model_config = ConfigDict(arbitrary_types_allowed=True, protected_namespaces=())
    
    client: Optional[httpx.Client] = None
    key: str = None
    name: Dict[str, str] | None = None
    description: Dict[str, str] | None = None
    label: str
    model_from: str
    model_to: str
    parameter_definition: list[dict] | None = None
    meta: Dict[str, Any] | None
        
    

    def resolve(self, entity: Entity, **kwargs) -> Dict[str, Any]:
        # The server expects the params as a JSON string in the body of the POST request
        response = self.client.post(
            url=f"relation/resolve/{self.key}/{entity.fb_entity_id}",
            json=kwargs  # `json=` will automatically encode `kwargs` as a JSON string
        )
        response.raise_for_status()  # This will raise an error for HTTP error responses
        return response.json()
        
        
    
