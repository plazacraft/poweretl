from dataclasses import dataclass, field

from poweretl.defs.model import Properties as mProperties
from poweretl.defs.model import Property as mProperty

from .base import BaseCollection, BaseItem


@dataclass
class Property(BaseItem, mProperty):
    pass


@dataclass
class Properties(BaseCollection, mProperties):
    items: dict[str, Property] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
