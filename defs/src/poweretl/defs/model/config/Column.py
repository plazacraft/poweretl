from dataclasses import dataclass
from .Base import *

@dataclass
class Column(Base):
    """ Column definition in table.
    """
    type:str
    properties: object = None