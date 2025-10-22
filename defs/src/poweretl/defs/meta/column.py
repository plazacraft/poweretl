from dataclasses import dataclass, field

from poweretl.defs.model import Column as mColumn, Columns as mColumns

from .base import MetaInfo, BaseItem


@dataclass
class Column(BaseItem, mColumn):
    pass


@dataclass
class Columns(mColumns):
    items: dict[str, Column] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
