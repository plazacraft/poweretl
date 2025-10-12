from dataclasses import dataclass
from .Base import * 

@dataclass
class PrimaryKey(Base):
    """ Primary key definition in table.
    """
    column_names:list[str]