from dataclasses import dataclass
from .Table import *



@dataclass
class Model:
    """ Represents configuration of model. 
    Attributes:
        tables (dict[str, Table], optional): Tables in the model.
    """
    params      :dict[str, str]             = field(default_factory=dict)
    columns     :dict[str, Column]          = field(default_factory=dict)
    column_sets :dict[str, list[str]]       = field(default_factory=dict)
    tables      :dict[str, Table]           = field(default_factory=dict)


