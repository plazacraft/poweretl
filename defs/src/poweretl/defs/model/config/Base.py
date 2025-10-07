from dataclasses import dataclass

@dataclass
class Base:
    """ Base class for all model entities.
    Attributes:
        id (str): Identification of the table.
        name (str): Full name of the table (together with schema, catalog, etc. - depends on the system).
    """
    name: str