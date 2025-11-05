from dataclasses import dataclass, field

from poweretl.defs.model import NameValue as mNameValue
from poweretl.defs.model import NameValues as mNameValues

from .base import BaseCollection, BaseItem


@dataclass
class NameValue(BaseItem, mNameValue):
    pass


@dataclass
class NameValues(BaseCollection, mNameValues):
    items: dict[str, NameValue] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
