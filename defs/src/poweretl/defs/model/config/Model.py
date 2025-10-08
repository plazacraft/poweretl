from dataclasses import dataclass
from .Table import *



@dataclass
class Model:
    """ Represents configuration of model. 
    Attributes:
        tables (dict[str, Table], optional): Tables in the model.
    """
    params: dict[str, str] = None
    columns:dict[str, Column] = None
    column_sets:dict[str, list[str]] = None
    tables:dict[str, Table] = None


