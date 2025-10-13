from dataclasses import dataclass, field

from .base import Base
from .column import Column


@dataclass
class Table(Base):
    """Table definition in model.
    Attributes:
        columns (dict[str, Column], optional): Columns in the table.
        properties (object, optional): Additional properties of the table.
    """

    columns: dict[str, Column] = field(default_factory=dict)
    properties: object = None
