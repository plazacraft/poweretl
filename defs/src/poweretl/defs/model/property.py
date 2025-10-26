from dataclasses import dataclass, field

from .base import BaseCollection, BaseItem


@dataclass
class Property(BaseItem):
    value: str = None


@dataclass
class Properties(BaseCollection):
    items: dict[str, Property] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
