from dataclasses import dataclass, field

from poweretl.defs.model import Table as mTable
from poweretl.defs.model import Tables as mTables

from .base import BaseCollection, BaseItem
from .column import Columns
from .tag import Tags


@dataclass
class Table(BaseItem, mTable):
    columns: Columns = field(default_factory=Columns)
    tags: Tags = field(default_factory=Tags)


@dataclass
class Tables(BaseCollection, mTables):
    items: dict[str, Table] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
