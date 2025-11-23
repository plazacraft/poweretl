from dataclasses import dataclass, field

from poweretl.defs.model import Model

from .table import Tables


@dataclass
class Meta(Model):

    tables: Tables = field(default_factory=Tables)
