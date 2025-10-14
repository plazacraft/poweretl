from dataclasses import dataclass

from poweretl.defs.model import Column as mColumn

from .meta_info import MetaInfo


@dataclass
class Column(mColumn):

    meta: MetaInfo = MetaInfo()
