from dataclasses import dataclass, field

from poweretl.defs.model import Model

from .table import Table


@dataclass
class Meta(Model):

    tables: dict[str, Table] = field(default_factory=dict)
