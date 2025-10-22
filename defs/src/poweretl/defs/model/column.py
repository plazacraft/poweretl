from dataclasses import dataclass, field

from .base import BaseItem, BaseCollection


@dataclass
class Column(BaseItem):
    """Column definition in table."""

    type: str
    properties: object = None


@dataclass
class Columns(BaseCollection):
    items: dict[str, Column] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
