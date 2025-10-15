from dataclasses import dataclass, field

from poweretl.defs.model import Table as mTable

from .column import Column
from .meta_info import MetaInfo


@dataclass
class Table(mTable):

    columns: dict[str, Column] = field(default_factory=dict)
    meta: MetaInfo = field(default_factory=MetaInfo())
