from typing import Any
from typing import ClassVar
from typing import Dict
from typing import List
from pydantic import Field

from fusionbase.entities.Entity import Entity
from fusionbase.models.location.Coordinate import Coordinate
from fusionbase.models.location.AddressComponent import AddressComponent
from fusionbase.models.location.LocationType import LocationType

from fusionbase.KnowledgeGraph import KnowledgeGraph

    
class Location(Entity):
    entity_type: str = Field(default="LOCATION")
    formatted_address: str | None = None
    coordinate: Coordinate | None = None
    location_level: int | None = None
    address_components: List[AddressComponent] | List[Dict[str, Any]] | None = None
    entity_subtype: LocationType | None = "LOCATION"
    elevation: float | None = None
    alternative_names: List[str] = Field(default_factory=list)
    #requests = None
    
    
    
    def answer(self, question: str) -> str:
        query = f"{question} for {self.formatted_address}"
        result = self.client.get(
            f"/search/fusion?q={query}"
        )
        result = result.json()
        
        if "knowledge_graph" in result:
            return KnowledgeGraph.get(client=self.client, **result["knowledge_graph"])      
        
    
    
    def find_data(self, indicator: str) -> bool:
        query = f"{indicator} for {self.formatted_address}"
        result = self.client.get(
            f"/search/fusion?q={query}"
        )
        result = result.json().get("items").get("relations")
        return result
