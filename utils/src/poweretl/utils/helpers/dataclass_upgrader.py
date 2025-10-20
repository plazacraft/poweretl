from dataclasses import fields, is_dataclass
from typing import Generic, Type, TypeVar, get_origin

T = TypeVar("T")


class DataclassUpgrader(Generic[T]):
    def __init__(self, child_cls: Type[T]):
        if not is_dataclass(child_cls):
            raise TypeError(f"{child_cls} must be a dataclass")
        self.child_cls = child_cls
        self._child_fields = fields(child_cls)

    def from_parent(self, parent_obj, **overrides) -> T:
        """
        Create an instance of the dataclass child from a parent object.
        - Copies all matching non-collection attributes from parent_obj.
        - Allows overrides for child-specific fields.
        """
        init_args = {}
        for f in self._child_fields:
            if not hasattr(parent_obj, f.name):
                continue

            # Detect if the field is a collection type
            origin = get_origin(f.type)
            if origin in (list, dict, set, tuple):
                continue  # skip collections

            init_args[f.name] = getattr(parent_obj, f.name)

        init_args.update(overrides)
        return self.child_cls(**init_args)

    def are_the_same(self, parent_obj, child_obj) -> bool:
        """
        Return True if all non-collection fields in child_obj
        have the same value as in parent_obj.
        """
        for f in self._child_fields:
            if not hasattr(parent_obj, f.name) or not hasattr(child_obj, f.name):
                continue

            origin = get_origin(f.type)
            if origin in (list, dict, set, tuple):
                continue  # skip collections

            if getattr(parent_obj, f.name) != getattr(child_obj, f.name):
                return False
        return True
