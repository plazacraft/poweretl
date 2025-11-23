# pylint: disable=W0212, R0915
# Mostly AI

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


def test_dataclass_upgrader_from_parent_and_compare():
    @dataclass
    class Parent:
        x: int = None
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

    # previous implementation used from_parent; current implementation
    # constructs a child instance and updates it in-place via update_child
    child = upgrader.child_cls()
    upgrader.update_child(parent, child)
    assert isinstance(child, Child)
    assert child.x == 10
    assert child.items == [1, 2, 3]
    assert child.only_in_child == "child_only"

    # deep copy: different object identity
    assert child.items is not parent.items
    # excluded field remains child's default
    assert child.secret == "child_default"

    # apply override by creating and then setting the field after update
    child2 = upgrader.child_cls()
    upgrader.update_child(parent, child2)
    child2.secret = "overridden"
    assert child2.secret == "overridden"

    # compare: child created from parent (ignoring excluded field) should be same
    assert upgrader.compare(parent, child) is True

    # if we change a non-excluded field, compare should return False
    child.items.append(99)
    assert upgrader.compare(parent, child) is False

    # Verify that changing only the excluded field still compares as True
    child.items.pop()  # reset items back
    child.secret = "completely_different"
    assert upgrader.compare(parent, child) is True


def test_dataclass_upgrader_init_raises_on_non_dataclass():
    with pytest.raises(TypeError):
        DataclassUpgrader(int)


def test_nested_excluded_field_comparison():
    @dataclass
    class NestedWithSecret:
        value: int
        secret: str = field(default="hidden", metadata={"exclude_from_upgrader": True})

        def __hash__(self):
            return hash(self.value)

    @dataclass
    class Parent:
        x: int
        nested: NestedWithSecret
        items: list[NestedWithSecret]
        secrets: dict[str, NestedWithSecret]
        secret_set: set[NestedWithSecret]

    @dataclass
    class Child(Parent):
        pass

    # Create two objects with different values in excluded fields
    parent = Parent(
        x=1,
        nested=NestedWithSecret(value=10, secret="parent_secret"),
        items=[
            NestedWithSecret(value=1, secret="secret1"),
            NestedWithSecret(value=2, secret="secret2"),
        ],
        secrets={"a": NestedWithSecret(value=3, secret="secret3")},
        secret_set={NestedWithSecret(value=4, secret="secret4")},
    )

    child = Parent(
        x=1,
        nested=NestedWithSecret(value=10, secret="different_secret"),
        items=[
            NestedWithSecret(value=1, secret="different1"),
            NestedWithSecret(value=2, secret="different2"),
        ],
        secrets={"a": NestedWithSecret(value=3, secret="different3")},
        secret_set={NestedWithSecret(value=4, secret="different4")},
    )

    upgrader = DataclassUpgrader(Child)

    # Objects should compare as equal because excluded fields are ignored
    # at all levels of nesting
    assert upgrader.compare(parent, child) is True

    # Modify a non-excluded field in nested object
    child.nested.value = 99
    assert upgrader.compare(parent, child) is False

    # Modify a non-excluded field in list item
    child.nested.value = 10  # reset
    child.items[0].value = 99
    assert upgrader.compare(parent, child) is False

    # Modify a non-excluded field in dict value
    child.items[0].value = 1  # reset
    child.secrets["a"].value = 99
    assert upgrader.compare(parent, child) is False

    # Modify a non-excluded field in set item
    child.secrets["a"].value = 3  # reset
    next(iter(child.secret_set)).value = 99
    assert upgrader.compare(parent, child) is False


def test_update_child():
    @dataclass
    class DeepNested:
        value: int
        secret: str = field(
            default="deep_secret", metadata={"exclude_from_upgrader": True}
        )

    @dataclass
    class MiddleNested:
        deep: DeepNested
        items: list[DeepNested]
        secret: str = field(
            default="middle_secret", metadata={"exclude_from_upgrader": True}
        )

    @dataclass
    class Parent:
        name: str
        middle: MiddleNested
        items_dict: dict[str, DeepNested]
        secret: str = field(
            default="parent_secret", metadata={"exclude_from_upgrader": True}
        )

    @dataclass
    class Child(Parent):
        child_value: int = 42

    # Create source object (can be parent or child)
    source_deep = DeepNested(value=1, secret="source_deep_secret")
    source = Parent(
        name="source",
        middle=MiddleNested(
            deep=source_deep,
            items=[DeepNested(value=i, secret=f"secret{i}") for i in range(2)],
            secret="source_middle_secret",
        ),
        items_dict={"key": DeepNested(value=5, secret="dict_secret")},
        secret="source_secret",
    )

    # Create destination object (must be child type)
    dest_deep = DeepNested(value=99, secret="dest_deep_secret")
    dest = Child(
        name="dest",
        middle=MiddleNested(
            deep=dest_deep,
            items=[DeepNested(value=99, secret="old_secret")],
            secret="dest_middle_secret",
        ),
        items_dict={"key": DeepNested(value=99, secret="old_dict_secret")},
        secret="dest_secret",
        child_value=100,
    )

    # Keep references to verify identity preservation
    original_middle = dest.middle
    original_deep = dest.middle.deep
    original_items = dest.middle.items
    original_dict = dest.items_dict

    upgrader = DataclassUpgrader(Child)
    upgrader.update_child(source, dest)

    # Verify object identity is preserved where possible
    assert dest.middle is original_middle
    assert dest.middle.deep is original_deep
    assert dest.middle.items is not original_items  # Lists are recreated
    assert dest.items_dict is not original_dict  # Dicts are recreated

    # Verify values are updated correctly
    assert dest.name == "source"
    assert dest.middle.deep.value == 1
    assert len(dest.middle.items) == 2
    assert dest.middle.items[0].value == 0
    assert dest.middle.items[1].value == 1
    assert dest.items_dict["key"].value == 5

    # Verify excluded fields retain their original values
    assert dest.secret == "dest_secret"
    assert dest.middle.secret == "dest_middle_secret"
    assert dest.middle.deep.secret == "dest_deep_secret"
    assert dest.middle.items[0].secret == "deep_secret"  # default value for new item
    assert dest.items_dict["key"].secret == "deep_secret"  # default value for new item

    # Verify child-specific fields are preserved
    assert dest.child_value == 100

    # Test updating from another Child instance
    source2 = Child(
        name="source2",
        middle=MiddleNested(
            deep=DeepNested(value=42, secret="new_secret"),
            items=[],
            secret="new_middle_secret",
        ),
        items_dict={},
        secret="new_secret",
        child_value=200,
    )

    upgrader.update_child(source2, dest)

    # Verify updates from child source work
    assert dest.name == "source2"
    assert dest.middle.deep.value == 42
    assert len(dest.middle.items) == 0
    assert len(dest.items_dict) == 0
    assert dest.child_value == 200

    # Verify excluded fields still maintain their values
    assert dest.secret == "dest_secret"
    assert dest.middle.secret == "dest_middle_secret"
    assert dest.middle.deep.secret == "dest_deep_secret"


def test_compare_deeply_nested_with_excludes():
    @dataclass
    class DeepNested:
        value: int
        secret: str = field(
            default="deep_secret", metadata={"exclude_from_upgrader": True}
        )

    @dataclass
    class MiddleNested:
        deep: DeepNested
        middle_value: str
        middle_secret: str = field(
            default="middle_secret", metadata={"exclude_from_upgrader": True}
        )
        deep_list: list[DeepNested] = field(default_factory=list)

    @dataclass
    class Parent:
        top_value: int
        middle: MiddleNested
        top_secret: str = field(
            default="top_secret", metadata={"exclude_from_upgrader": True}
        )
        middle_dict: dict[str, MiddleNested] = field(default_factory=dict)

    @dataclass
    class Child(Parent):
        child_only: int = 42

    # Create deeply nested objects with excluded fields at each level
    deep1 = DeepNested(value=1, secret="secret1")
    deep2 = DeepNested(value=2, secret="secret2")
    middle = MiddleNested(
        deep=deep1,
        middle_value="middle1",
        middle_secret="middlesecret1",
        deep_list=[deep1, deep2],
    )

    parent = Parent(
        top_value=100,
        middle=middle,
        top_secret="topsecret1",
        middle_dict={"key": middle},
    )

    # Create child with different values in excluded fields but same in non-excluded
    child_deep1 = DeepNested(value=1, secret="different_secret1")
    child_deep2 = DeepNested(value=2, secret="different_secret2")
    child_middle = MiddleNested(
        deep=child_deep1,
        middle_value="middle1",  # Same as parent
        middle_secret="different_middlesecret1",
        deep_list=[child_deep1, child_deep2],
    )

    child = Child(
        top_value=100,  # Same as parent
        middle=child_middle,
        top_secret="different_topsecret1",
        middle_dict={"key": child_middle},
    )

    upgrader = DataclassUpgrader(Child)

    # Should be equal despite different values in excluded fields at all levels
    assert upgrader.compare(parent, child) is True

    # Change a non-excluded field at each level and verify comparison fails

    # Top level change
    child.top_value = 999
    assert upgrader.compare(parent, child) is False
    child.top_value = 100  # reset

    # Middle level change
    child.middle.middle_value = "different_middle"
    assert upgrader.compare(parent, child) is False
    child.middle.middle_value = "middle1"  # reset

    # Deep level change
    child.middle.deep.value = 999
    assert upgrader.compare(parent, child) is False
    child.middle.deep.value = 1  # reset

    # Container item change
    child.middle.deep_list[0].value = 999
    assert upgrader.compare(parent, child) is False
    child.middle.deep_list[0].value = 1  # reset

    # Dictionary value change
    child.middle_dict["key"].middle_value = "different"
    assert upgrader.compare(parent, child) is False
    child.middle_dict["key"].middle_value = "middle1"  # reset

    # Verify excluded fields can be different at all levels and still compare equal
    child.top_secret = "completely_different_top"
    child.middle.middle_secret = "completely_different_middle"
    child.middle.deep.secret = "completely_different_deep"
    child.middle.deep_list[0].secret = "completely_different_list_item"
    child.middle_dict["key"].deep.secret = "completely_different_dict_value"
    assert upgrader.compare(parent, child) is True


def test_update_child_2():
    @dataclass
    class Inner:
        value: int
        shared: list[int]
        secret: str = field(default="secret", metadata={"exclude_from_upgrader": True})

    @dataclass
    class Parent:
        name: str
        inner: Inner
        values: dict[str, Inner]
        items: list[Inner] = field(default_factory=list)
        shared_ref: Inner = None
        secret: str = field(
            default="parent_secret", metadata={"exclude_from_upgrader": True}
        )

    @dataclass
    class Child(Parent):
        extra: int = 42

    # Create initial child object with nested structure
    child_inner = Inner(value=1, shared=[1, 2], secret="child_inner_secret")
    child = Child(
        name="initial",
        inner=child_inner,
        values={"a": Inner(value=10, shared=[5], secret="secret_a")},
        items=[Inner(value=20, shared=[6], secret="secret_item")],
        shared_ref=child_inner,  # Same instance as inner
        secret="child_secret",
        extra=100,
    )

    # Create source object (can be either Parent or Child)
    source_inner = Inner(value=50, shared=[7, 8, 9], secret="source_secret")
    source = Parent(
        name="source",
        inner=source_inner,
        values={"b": Inner(value=60, shared=[10], secret="new_secret")},
        items=[
            Inner(value=70, shared=[11], secret="new_item1"),
            Inner(value=80, shared=[12], secret="new_item2"),
        ],
        shared_ref=source_inner,
        secret="source_parent_secret",
    )

    # Keep references to verify identity preservation
    original_inner = child.inner
    original_inner_shared = child.inner.shared
    original_values = child.values
    original_items = child.items

    upgrader = DataclassUpgrader(Child)

    # Update child from source
    upgrader.update_child(source, child)

    # Verify object identity is preserved for dataclass instances where possible
    assert child.inner is original_inner  # Same instance
    assert child.shared_ref is child.inner  # Reference relationship maintained

    # But containers should be new instances
    assert child.inner.shared is not original_inner_shared
    assert child.values is not original_values
    assert child.items is not original_items

    # Verify values are updated correctly
    assert child.name == "source"
    assert child.inner.value == 50
    assert child.inner.shared == [7, 8, 9]
    assert len(child.values) == 1  # Should replace dict content
    assert "b" in child.values
    assert child.values["b"].value == 60
    assert len(child.items) == 2
    assert child.items[0].value == 70
    assert child.items[1].value == 80

    # Verify excluded fields retain their original values
    assert child.secret == "child_secret"  # Kept original
    assert child.inner.secret == "child_inner_secret"  # Kept original
    assert child.values["b"].secret == "secret"  # New instance gets default
    assert child.items[0].secret == "secret"  # New instance gets default

    # Child-specific fields should be preserved
    assert child.extra == 100

    # Test updating from another Child instance
    source2 = Child(
        name="source2",
        inner=Inner(value=90, shared=[13], secret="source2_secret"),
        values={},
        items=[],
        shared_ref=None,
        secret="source2_secret",
        extra=200,
    )

    upgrader.update_child(source2, child)

    # Verify updates from child source work
    assert child.name == "source2"
    assert child.inner.value == 90
    assert child.inner.shared == [13]
    assert len(child.values) == 0
    assert len(child.items) == 0
    assert child.shared_ref is None  # None in source2, should be updated
    assert child.extra == 200

    # Excluded fields should still maintain their original values
    assert child.secret == "child_secret"
    assert child.inner.secret == "child_inner_secret"

    # Test updating with None values
    source3 = Child(
        name="source3",
        inner=Inner(value=100, shared=[], secret="source3_secret"),
        values=None,  # None for container
        items=None,  # None for container
        shared_ref=None,  # None for reference
        secret="source3_secret",
        extra=300,
    )

    upgrader.update_child(source3, child)

    # Verify None handling
    assert child.name == "source3"
    assert child.values is None
    assert child.items is None
    assert child.shared_ref is None
    assert child.extra == 300
    # But inner object should still be updated (not replaced with None)
    assert child.inner is original_inner
    assert child.inner.value == 100
    assert not child.inner.shared
    assert child.inner.secret == "child_inner_secret"  # Still preserved


def test_nested_dataclass_and_container_copying():
    @dataclass
    class Inner:
        a: int = None
        secret: int = field(default=0, metadata={"exclude_from_upgrader": True})

    @dataclass
    class Parent:
        x: int = None
        inner: Inner = field(default_factory=Inner)
        inners: list = field(default_factory=list)

    @dataclass
    class Child(Parent):
        pass

    parent = Parent(
        x=1,
        inner=Inner(a=5, secret=99),
        inners=[Inner(a=1, secret=9), Inner(a=2, secret=8)],
    )

    upgrader = DataclassUpgrader(Child)

    child = upgrader.child_cls()
    upgrader.update_child(parent, child)

    # nested dataclass instance should be reconstructed (different identity)
    assert child.inner is not parent.inner
    assert isinstance(child.inner, Inner)
    assert child.inner.a == 5
    # excluded field on nested dataclass should not be copied and should use the default
    assert child.inner.secret == 0

    # container of dataclasses should be deep-copied and inner items reconstructed
    assert child.inners is not parent.inners
    assert child.inners[0] is not parent.inners[0]
    assert child.inners[0].a == 1
    assert child.inners[0].secret == 0
