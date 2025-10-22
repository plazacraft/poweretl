# pylint: disable=W0212

from dataclasses import dataclass, field

import pytest

from poweretl.utils.helpers.dataclass_upgrader import DataclassUpgrader


def test_is_excluded_with_class_and_instance_and_non_dataclass():

    @dataclass
    class Parent:
        a: int = field(default=1, metadata={"exclude_from_upgrader": True})
        b: int = 2

    @dataclass
    class Child(Parent):
        pass

    upgrader = DataclassUpgrader(Child)

    # Class-level check
    assert upgrader._is_excluded(Parent, "a") is True
    assert upgrader._is_excluded(Parent, "b") is False

    # Instance-level check
    p = Parent()
    assert upgrader._is_excluded(p, "a") is True
    assert upgrader._is_excluded(p, "b") is False

    # Non-dataclass input should return False
    assert upgrader._is_excluded(object(), "a") is False


def test_dataclass_upgrader_from_parent_and_are_the_same():
    @dataclass
    class Parent:
        x: int
        items: list = field(default_factory=lambda: [1, 2, 3])
        secret: str = field(default="top", metadata={"exclude_from_upgrader": True})

    @dataclass
    class Child(Parent):
        # secret is excluded when creating from parent, so give child a default
        secret: str = field(default="child_default")
        only_in_child: str = "child_only"

    parent = Parent(x=10, items=[1, 2, 3], secret="parent_secret")

    # DataclassUpgrader must accept a dataclass type
    upgrader = DataclassUpgrader(Child)

    # from_parent should deep-copy items and not copy 'secret'
    child = upgrader.from_parent(parent)
    assert isinstance(child, Child)
    assert child.x == 10
    assert child.items == [1, 2, 3]
    assert child.only_in_child == "child_only"

    # deep copy: different object identity
    assert child.items is not parent.items
    # excluded field remains child's default
    assert child.secret == "child_default"

    # overrides should apply
    child2 = upgrader.from_parent(parent, secret="overridden")
    assert child2.secret == "overridden"

    # are_the_same: child created from parent (ignoring excluded field) should be same
    assert upgrader.are_the_same(parent, child) is True

    # if we change a non-excluded field, are_the_same should return False
    child.items.append(99)
    assert upgrader.are_the_same(parent, child) is False


def test_dataclass_upgrader_init_raises_on_non_dataclass():
    with pytest.raises(TypeError):
        DataclassUpgrader(int)
