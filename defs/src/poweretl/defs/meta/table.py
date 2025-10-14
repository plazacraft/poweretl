from dataclasses import dataclass, field

from poweretl.defs.model import Table as mTable

from .column import Column


@dataclass
class Table(mTable):

    columns: dict[str, Column] = field(default_factory=dict)
