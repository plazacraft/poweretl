from dataclasses import dataclass, field

from poweretl.defs.model import Column as mColumn
from poweretl.defs.model import Columns as mColumns

from .base import BaseItem, BaseCollection


@dataclass
class Column(BaseItem, mColumn):
    pass


@dataclass
class Columns(BaseCollection, mColumns):
    items: dict[str, Column] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
