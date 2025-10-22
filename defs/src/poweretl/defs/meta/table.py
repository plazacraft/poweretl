from dataclasses import dataclass, field

from poweretl.defs.model import Table as mTable, Tables as mTables

from .column import Columns
from .base import BaseItem


@dataclass
class Table(BaseItem, mTable):
    columns: Columns = field(
        default_factory=Columns
    )
    pass


@dataclass
class Tables(mTables):
    items: dict[str, Table] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
