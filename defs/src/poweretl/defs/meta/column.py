from dataclasses import dataclass, field

from poweretl.defs.model import Column as mColumn
from poweretl.defs.model import Columns as mColumns

from .base import BaseCollection, BaseItem
from .name_value import NameValues


@dataclass
class Column(BaseItem, mColumn):
    tags: NameValues = field(default_factory=NameValues)


@dataclass
class Columns(BaseCollection, mColumns):
    items: dict[str, Column] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
