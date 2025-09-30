from dataclasses import dataclass
from .Base import *
from .Column import *


@dataclass
class Table(Base):
    """ Table definition in model.
    Attributes:
        columns (dict[str, Column], optional): Columns in the table.
        properties (object, optional): Additional properties of the table.
    """
    columns:dict[str, Column] = None
    properties: object = None

