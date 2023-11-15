from pydantic import BaseModel

from fusionbase.entities.Location import Location

class RegistrationData(BaseModel):
	registration_authority_id: str
	registration_authority_name: str
	registration_authority_entity_id: str
	registration_authority_entity_name: str
	registration_type: str | None
	registration_number: str
	registration_id_extra: str | None
	registration_authority_location: Location | None

	def model_dump(self, **kwargs):
		reg_auth_dump = self.registration_authority_location.model_dump(**kwargs) if self.registration_authority_location is not None else None
		return {'registration_authority': {'local': {
			'registration_authority_id': self.registration_authority_id,
			'registration_authority_name': self.registration_authority_name,
			'registration_authority_entity_id': self.registration_authority_entity_id,
			'registration_authority_entity_name': self.registration_authority_entity_name,
			'registration_type': self.registration_type,
			'registration_number': self.registration_number,
			'registration_id_extra': self.registration_id_extra,
			'registration_authority_location': reg_auth_dump
		}}}
