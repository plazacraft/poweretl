from dataclasses import dataclass

from .base import Base


@dataclass
class ForeignKey(Base):
    """Foreign key definition in table."""

    source_column_names: list[str]
    target_column_names: list[str]
