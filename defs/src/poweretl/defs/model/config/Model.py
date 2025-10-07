from dataclasses import dataclass
from .Table import *



@dataclass
class Model:
    """ Represents configuration of model. 
    Attributes:
        tables (dict[str, Table], optional): Tables in the model.
    """
    tables:dict[str, Table] = None


