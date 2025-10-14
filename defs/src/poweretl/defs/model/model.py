from dataclasses import dataclass, field

from .table import Table


@dataclass
class Model:
    """Represents configuration of model.
    Attributes:
        prune_tables: Force removal of tables if don't exists in configuration.
        tables (dict[str, Table], optional): Tables in the model.
    """

    prune_tables: bool = False
    tables: dict[str, Table] = field(default_factory=dict)
