from dataclasses import dataclass, field

from poweretl.defs.model import Tag as mTag
from poweretl.defs.model import Tags as mTags

from .base import BaseCollection, BaseItem


@dataclass
class Tag(BaseItem, mTag):
    pass


@dataclass
class Tags(BaseCollection, mTags):
    items: dict[str, Tag] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
