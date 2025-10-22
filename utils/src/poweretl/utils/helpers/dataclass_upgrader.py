import copy
from dataclasses import fields, is_dataclass
from typing import Any, Generic, Type, TypeVar

from deepdiff import DeepDiff

T = TypeVar("T")


class DataclassUpgrader(Generic[T]):
    def __init__(self, child_cls: Type[T]):
        if not is_dataclass(child_cls):
            raise TypeError(f"{child_cls} must be a dataclass")
        self.child_cls = child_cls
        self._child_fields = fields(child_cls)

    def _is_excluded(self, obj: Any, field_name: str) -> bool:
        """Return True if the dataclass field named ``field_name`` on ``obj``
        (or on obj's class) has metadata key ``exclude_from_upgrader`` set to True.

        Accepts either a dataclass instance or a dataclass class. If the provided
        object is not a dataclass (class or instance) the function returns False.
        """
        # Accept either a class or an instance
        cls = obj if isinstance(obj, type) else getattr(obj, "__class__", None)
        if cls is None or not is_dataclass(cls):
            return False

        # dataclasses.fields includes fields from base classes as well
        for f in fields(cls):
            if f.name == field_name:
                # metadata behaves like a mapping; default to False
                return bool(f.metadata.get("exclude_from_upgrader", False))

        return False

    def from_parent(self, parent_obj, **overrides) -> T:
        """
        Create an instance of the dataclass child from a parent object.
        - Deep copies all attributes from parent_obj unless marked with @exclude.
        - Allows overrides for child-specific fields.
        """
        def _copy_dataclass_instance(obj):
            """Reconstruct a dataclass instance by copying its fields but
            skipping any fields marked with metadata 'exclude_from_upgrader'.
            This avoids using copy.deepcopy on the dataclass as a whole so
            excluded fields can be omitted.
            """
            if obj is None:
                return None

            cls = obj if isinstance(obj, type) else getattr(obj, "__class__", None)
            if cls is None or not is_dataclass(cls):
                # fallback to deepcopy for non-dataclass objects
                return copy.deepcopy(obj)

            result_kwargs = {}
            for f in fields(cls):
                name = f.name
                if not hasattr(obj, name):
                    continue

                # Skip fields that are marked excluded on the object's class
                if self._is_excluded(obj, name):
                    continue

                val = getattr(obj, name)
                result_kwargs[name] = _deep_copy_value(val)

            return cls(**result_kwargs)

        def _deep_copy_value(value):
            """Recursively copy values, reconstructing dataclass instances via
            _copy_dataclass_instance and falling back to deepcopy for other
            objects. Handles containers (list/tuple/set/dict).
            """
            # dataclass instance -> reconstruct without excluded fields
            if is_dataclass(value):
                return _copy_dataclass_instance(value)

            # Containers
            if isinstance(value, dict):
                return { _deep_copy_value(k): _deep_copy_value(v) for k, v in value.items() }

            if isinstance(value, list):
                return [ _deep_copy_value(v) for v in value ]

            if isinstance(value, tuple):
                return tuple(_deep_copy_value(v) for v in value)

            if isinstance(value, set):
                return { _deep_copy_value(v) for v in value }

            # Fallback to deepcopy for other types (primitives, class instances, etc.)
            return copy.deepcopy(value)

        init_args = {}
        for f in self._child_fields:
            if not hasattr(parent_obj, f.name):
                continue

            # Check if field should be excluded from copying
            if self._is_excluded(parent_obj, f.name):
                continue

            value = getattr(parent_obj, f.name)
            # Use our recursive copier which will reconstruct dataclasses
            init_args[f.name] = _deep_copy_value(value)

        init_args.update(overrides)
        return self.child_cls(**init_args)

    def are_the_same(self, parent_obj, child_obj) -> bool:
        """
        Return True if all non-excluded fields in child_obj have the same
        value as in parent_obj.
        Collections are deeply compared using DeepDiff.
        Excluded properties (marked with @exclude) are skipped in comparison.
        """
        for f in self._child_fields:
            if not hasattr(parent_obj, f.name) or not hasattr(child_obj, f.name):
                continue

            # Skip excluded properties in comparison
            if self._is_excluded(parent_obj, f.name):
                continue

            parent_val = getattr(parent_obj, f.name)
            child_val = getattr(child_obj, f.name)

            # Use DeepDiff to compare values - if there are any differences,
            # values are not equal
            if DeepDiff(parent_val, child_val):
                return False

        return True
