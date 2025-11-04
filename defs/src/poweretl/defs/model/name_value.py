from dataclasses import dataclass, field

from .base import BaseCollection, BaseItem


@dataclass
class NameValue(BaseItem):
    value: str = None


@dataclass
class NameValues(BaseCollection):
    items: dict[str, NameValue] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
