from dataclasses import dataclass, field

from .base import Base
from .column import Column


@dataclass
class Table(Base):
    """Table definition in model.
    Attributes:
        prune_columns: Force removal of columns from table
            if don't exists in configuration.
        columns (dict[str, Column], optional): Columns in the table.
        properties (object, optional): Additional properties of the table.
    """

    prune_columns: bool = False
    columns: dict[str, Column] = field(default_factory=dict, metadata={"exclude_from_upgrader": True})
    properties: object = None
