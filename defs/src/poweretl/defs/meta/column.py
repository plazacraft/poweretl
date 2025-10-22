from dataclasses import dataclass, field

from poweretl.defs.model import Column as mColumn

from .meta_info import MetaInfo


@dataclass
class Column(mColumn):

    meta: MetaInfo = field(default_factory=MetaInfo)
