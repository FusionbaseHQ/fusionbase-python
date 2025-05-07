"""Source information module."""

from typing import Any, Dict, Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class Source(BaseModel):
    """Source information for Fusionbase entities and services.

    This class represents source information for both entity data and services,
    with flexible properties to accommodate different source structures.

    Attributes:
        id: Source identifier
        service_specific: Additional service-specific information
        uri: URI for the source
        internal_id: Internal source identifier
    """
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,  # Allow population by field name (without alias)
    )

    id: Optional[str] = None
    internal_id: Optional[str] = Field(None, alias="_id")
    service_specific: Optional[Dict[str, Any]] = None
    uri: Optional[str] = Field(None, description="URI for the source")

    @property
    def source_id(self) -> Optional[str]:
        """Get the source identifier (id or internal_id)."""
        return self.id or self.internal_id
