from dataclasses import dataclass, field

from .table import Tables


@dataclass
class Model:
    """Represents configuration of model.
    Attributes:
        tables (dict[str, Table], optional): Tables in the model.
    """

    tables: Tables = field(default_factory=Tables)
