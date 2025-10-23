from dataclasses import dataclass, field

from poweretl.defs.model import Table as mTable
from poweretl.defs.model import Tables as mTables

from .base import BaseItem
from .column import Columns


@dataclass
class Table(BaseItem, mTable):
    columns: Columns = field(default_factory=Columns)


@dataclass
class Tables(mTables):
    items: dict[str, Table] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
