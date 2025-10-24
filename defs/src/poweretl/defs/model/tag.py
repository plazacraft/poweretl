from dataclasses import dataclass, field

from .base import BaseCollection, BaseItem


@dataclass
class Tag(BaseItem):
    """Column definition in table."""

    key: str = None
    value: str = None


@dataclass
class Tags(BaseCollection):
    items: dict[str, Tag] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
