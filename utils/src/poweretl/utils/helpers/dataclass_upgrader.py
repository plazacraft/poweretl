from dataclasses import fields, is_dataclass
from typing import Generic, Type, TypeVar

T = TypeVar("T")


class DataclassUpgrader(Generic[T]):
    def __init__(self, child_cls: Type[T]):
        if not is_dataclass(child_cls):
            raise TypeError(f"{child_cls} must be a dataclass")
        self.child_cls = child_cls
        self.field_names = {f.name for f in fields(child_cls)}

    def from_parent(self, parent_obj, **overrides) -> T:
        """
        Create an instance of the dataclass child from a parent object.
        - Copies all matching attributes from parent_obj.
        - Allows overrides for child-specific fields.
        """
        init_args = {
            name: getattr(parent_obj, name)
            for name in self.field_names
            if hasattr(parent_obj, name)
        }
        init_args.update(overrides)
        return self.child_cls(**init_args)
