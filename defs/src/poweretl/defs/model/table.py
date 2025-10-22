from dataclasses import dataclass, field

from .base import BaseItem, BaseCollection
from .column import Columns

@dataclass
class Table(BaseItem):
    """Table definition in model.
    Attributes:
        prune_columns: Force removal of columns from table
            if don't exists in configuration.
        columns (dict[str, Column], optional): Columns in the table.
        properties (object, optional): Additional properties of the table.
    """
    columns: Columns = field(
        default_factory=Columns
    )
    properties: object = None


@dataclass
class Tables(BaseCollection):
    items: dict[str, Table] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
