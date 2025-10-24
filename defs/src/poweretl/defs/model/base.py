from dataclasses import dataclass, field


@dataclass
class BaseItem:
    """Base class for all model entities.
    Attributes:
        name (str): Full name of the entity
    """

    name: str = None


@dataclass
class BaseCollection:
    """Base class for all model collections.
    Attributes:
        id (str): Identification of the table.
        name (str): Full name of the table (together with schema,
            catalog, etc. - depends on the system).
    """

    prune: bool = field(default=False)
    items: dict[str, BaseItem] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
