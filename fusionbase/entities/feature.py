"""Feature entity module."""

from typing import Any, ClassVar, Dict, List, Optional, Union

from fusionbase.entities.base import Entity
from fusionbase.types.entities import EntityType


class Feature(Entity):
    """Feature entity in Fusionbase.

    Features represent special entities that are typically related to other entities
    and provide additional data points such as statistical indicators.

    Unlike regular entities, features are not loaded through from_id methods and
    don't support search functionality. They're primarily used in relations.

    Attributes:
        fb_entity_id: Unique identifier for the feature
        value: The feature's value data which can be of various types
    """
    # Entity type information using the enum
    entity_type: ClassVar[EntityType] = EntityType.FEATURE
    entity_subtype: str = "FEATURE"

    value: Optional[Union[Dict[str, Any], List[Any], str]] = None

    @classmethod
    def _from_id(cls, client, entity_id: str) -> "Feature":
        """Internal method to create a Feature instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request
        data = make_entity_request(client, cls.entity_type.value, entity_id)
        return cls.model_validate(data)

    @classmethod
    async def _afrom_id(cls, client, entity_id: str) -> "Feature":
        """Asynchronously create a Feature instance by fetching it from the API."""
        from fusionbase.utils.api_utils import make_entity_request_async
        data = await make_entity_request_async(client, cls.entity_type.value,
                                               entity_id)
        return cls.model_validate(data)
