from dataclasses import dataclass, field
from typing import Optional

from .base import BaseCollection, BaseItem
from .column import Columns
from .tag import Tags
from .property import Properties

@dataclass
class Table(BaseItem):
    """Table definition in model.
    Attributes:
        prune_columns: Force removal of columns from table
            if don't exists in configuration.
        columns (dict[str, Column], optional): Columns in the table.
        properties (object, optional): Additional properties of the table.
    """

    external_location: Optional[str] = None
    comment: Optional[str] = None
    cluster_by: list = field(default_factory=list)
    columns: Columns = field(default_factory=Columns)
    tags: Tags = field(default_factory=Tags)
    properties: Properties = field(default_factory=Properties)



@dataclass
class Tables(BaseCollection):
    items: dict[str, Table] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
