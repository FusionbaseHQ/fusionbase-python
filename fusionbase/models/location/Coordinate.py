from pydantic import BaseModel


class Coordinate(BaseModel):
    latitude: float
    longitude: float