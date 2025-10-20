from dataclasses import dataclass

import pytest

from poweretl.utils import DataclassUpgrader

# --- Sample classes for testing ---


class Base:
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.custom_list = ["test_1", "test_2"]


@dataclass
class Child(Base):
    a: int
    b: int
    c: int = 0
    d: str = "default"
    custom_list: list[str] = None


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
    assert child.custom_list is None
    assert upgrader.are_the_same(base, child)


def test_override_fields():
    base = Base(10, 20)
    upgrader = DataclassUpgrader(Child)
    child = upgrader.from_parent(base, c=99, d="custom")
    child.a = 30

    assert child.c == 99
    assert child.d == "custom"
    assert not upgrader.are_the_same(base, child)


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
