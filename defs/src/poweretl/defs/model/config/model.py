from dataclasses import dataclass, field

from .table import Table


@dataclass
class Model:
    """Represents configuration of model.
    Attributes:
        tables (dict[str, Table], optional): Tables in the model.
    """

    tables: dict[str, Table] = field(default_factory=dict)
