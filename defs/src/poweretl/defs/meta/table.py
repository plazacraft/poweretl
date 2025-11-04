from dataclasses import dataclass, field

from poweretl.defs.model import Table as mTable
from poweretl.defs.model import Tables as mTables

from .base import BaseCollection, BaseItem
from .column import Columns
from .name_value import NameValues



@dataclass
class Table(BaseItem, mTable):
    columns: Columns = field(default_factory=Columns)
    tags: NameValues = field(default_factory=NameValues)
    properties: NameValues = field(default_factory=NameValues)
    settings: NameValues = field(default_factory=NameValues)

@dataclass
class Tables(BaseCollection, mTables):
    items: dict[str, Table] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
