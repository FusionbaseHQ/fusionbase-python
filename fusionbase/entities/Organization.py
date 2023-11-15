from fusionbase.entities.Entity import Entity
from fusionbase.entities.Location import Location
from fusionbase.models.organization.OrganizationState import OrganizationState
from fusionbase.models.organization.OrganizationState import OrganizationStatus
from fusionbase.models.organization.RegistrationData import RegistrationData
from fusionbase.models.organization.Classifications import Classifications
from fusionbase.models.organization.Contact import Contact



from typing import Dict
from typing import Optional
from datetime import datetime
from datetime import date
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


class Organization(Entity):
    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    entity_type: str = Field(default="ORGANIZATION")
    entity_subtype: str = Field(default="CORPORATION")  # "KAPITALGESELLSCHAFT"


    external_ids: dict | None = None
    status: OrganizationState = OrganizationState(active=True, status=OrganizationStatus('UNKNOWN'))
    
    name: str | None = None
    other_names: list[str] | None = None
    description: dict | None = None
    
    address: Location | None = None
    other_addresses: list[dict] | None = None
    
    jurisdiction: dict | None = None
    contact: Optional[Contact] = None
    
    # events: list[dict]
    founding_date: datetime | date | None = None
    cessation_date: datetime | date | None = None

    legal: RegistrationData | None = None
    # financials: dict | None

    classifications: Optional[Classifications] = None
    source: Dict[str, str] | None = None
    
    @field_validator('legal', mode='before')
    @classmethod
    def convert_to_model(cls, value):
        if isinstance(value, RegistrationData):
            return value
        elif isinstance(value, dict):
            local = value['registration_authority']['local']
            court_address = local.pop('registration_authority_location')

            if isinstance(court_address, Location):
                pass
            elif court_address is None:
                pass
            else:
                court_address = Location.model_validate(court_address)

            return RegistrationData.model_validate({**local, 'registration_authority_location': court_address})
        else:
            raise ValueError(f'Unexpected value for legal: {type(value)}, expected dict or RegistrationData')
        
        
        
    
