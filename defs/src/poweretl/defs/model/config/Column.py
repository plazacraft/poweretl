from dataclasses import dataclass

from .base import Base


@dataclass
class Column(Base):
    """Column definition in table."""

    type: str
    properties: object = None
