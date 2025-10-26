from dataclasses import dataclass, field

from .base import BaseCollection, BaseItem
from .tag import Tags


@dataclass
class Column(BaseItem):
    """Column definition in table."""

    type: str = None
    tags: Tags = field(default_factory=Tags)
    properties: object = None


@dataclass
class Columns(BaseCollection):
    items: dict[str, Column] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
