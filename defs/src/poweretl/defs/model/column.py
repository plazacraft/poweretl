from dataclasses import dataclass, field
from typing import Optional

from .base import BaseCollection, BaseItem
from .name_value import NameValues


@dataclass
class Column(BaseItem):
    """Column definition in table."""

    type: str = None
    comment: Optional[str] = None
    tags: NameValues = field(default_factory=NameValues)
    properties: object = None


@dataclass
class Columns(BaseCollection):
    items: dict[str, Column] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
