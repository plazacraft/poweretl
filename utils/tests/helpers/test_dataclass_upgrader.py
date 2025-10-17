from dataclasses import dataclass

import pytest

from poweretl.utils import DataclassUpgrader

# --- Sample classes for testing ---


class Base:
    def __init__(self, a, b):
        self.a = a
        self.b = b


@dataclass
class Child(Base):
    a: int
    b: int
    c: int = 0
    d: str = "default"


# --- Tests ---


def test_basic_upgrade():
    base = Base(1, 2)
    upgrader = DataclassUpgrader(Child)
    child = upgrader.from_parent(base)

    assert isinstance(child, Child)
    assert child.a == 1
    assert child.b == 2
    assert child.c == 0
    assert child.d == "default"


def test_override_fields():
    base = Base(10, 20)
    upgrader = DataclassUpgrader(Child)
    child = upgrader.from_parent(base, c=99, d="custom")

    assert child.c == 99
    assert child.d == "custom"


def test_missing_field_in_parent():
    class Incomplete:
        def __init__(self):
            self.a = 5  # b is missing

    upgrader = DataclassUpgrader(Child)
    with pytest.raises(TypeError):
        upgrader.from_parent(Incomplete())


def test_non_dataclass_child():
    class NotADataclass:
        pass

    with pytest.raises(TypeError):
        DataclassUpgrader(NotADataclass)
