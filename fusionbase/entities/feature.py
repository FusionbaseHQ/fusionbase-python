"""Feature entity module."""

from typing import Any, ClassVar, Dict, List, Union

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

    value: Union[Dict[str, Any], List[Any], str, None] = None
