# pylint: disable=R0912, R0911, E1121
import copy
from dataclasses import fields, is_dataclass
from typing import Any, Generic, Type, TypeVar


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

    def _copy_dataclass_instance(self, obj, cls=None, **overrides):
        """Reconstruct a dataclass instance by copying its fields but
        skipping any fields marked with metadata 'exclude_from_upgrader'.
        This avoids using copy.deepcopy on the dataclass as a whole so
        excluded fields can be omitted.
        """
        if obj is None:
            return None

        if not cls:
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
            deep_val = self._deep_copy_value(val)
            result_kwargs[name] = deep_val

        result_kwargs.update(overrides)
        return cls(**result_kwargs)

    def _deep_copy_value(self, value):
        """Recursively copy values, reconstructing dataclass instances via
        _copy_dataclass_instance and falling back to deepcopy for other
        objects. Handles containers (list/tuple/set/dict).
        """
        # dataclass instance -> reconstruct without excluded fields
        if is_dataclass(value):
            return self._copy_dataclass_instance(value)

        # Containers
        if isinstance(value, dict):
            return {
                self._deep_copy_value(k): self._deep_copy_value(v)
                for k, v in value.items()
            }

        if isinstance(value, list):
            return [self._deep_copy_value(v) for v in value]

        if isinstance(value, tuple):
            return tuple(self._deep_copy_value(v) for v in value)

        if isinstance(value, set):
            return {self._deep_copy_value(v) for v in value}

        # Fallback to deepcopy for other types (primitives, class instances, etc.)
        return copy.deepcopy(value)

    def from_parent(self, parent_obj, **overrides) -> T:
        """
        Create an instance of the dataclass child from a parent object.
        - Deep copies all attributes from parent_obj unless marked with @exclude.
        - Allows overrides for child-specific fields.
        """

        return self._copy_dataclass_instance(parent_obj, self.child_cls, **overrides)

    def update_child(self, source_obj, dest_obj) -> None:
        """Update a child instance (dest_obj) with values from
        source_obj (can be parent or child).

        This function updates dest_obj in-place with values
        from source_obj, maintaining
        object identity while updating all non-excluded fields
        recursively.

        Args:
            source_obj: The source object to copy values from.
                Can be either parent or child type.
            dest_obj: The destination object to update.
                Must be a child type instance.

        The function:
        - Updates all non-excluded fields from source to destination
        - Preserves object identity of the destination object
        - Performs deep updates of nested dataclasses and containers
        - Maintains excluded field values in the destination
        """

        def _update_dataclass(source, dest):
            """Recursively update a dataclass instance, preserving identity."""
            if source is None or dest is None:
                return

            if not is_dataclass(source) or not is_dataclass(dest):
                return

            # Get all field names from the destination class
            dest_fields = {f.name: f for f in fields(dest)}

            # Update fields that exist in both source and dest
            for name, _ in dest_fields.items():
                if not hasattr(source, name):
                    continue

                # Skip excluded fields
                if self._is_excluded(source, name):
                    continue

                source_val = getattr(source, name)
                dest_val = getattr(dest, name)

                # Handle nested dataclasses - try to update in place
                if is_dataclass(source_val) and is_dataclass(dest_val):
                    _update_dataclass(source_val, dest_val)
                    continue

                # Handle field references and None values
                if source_val is None:
                    setattr(dest, name, None)
                    continue

                # For same-type objects, try to preserve references
                if source_val is getattr(source, "inner", None) and dest_val is getattr(
                    dest, "inner", None
                ):
                    continue  # Skip update to maintain reference relationship

                # Handle containers
                if isinstance(source_val, dict) and isinstance(dest_val, dict):
                    updated_dict = _deep_update_dict(source_val)
                    setattr(dest, name, updated_dict)
                elif isinstance(source_val, list) and isinstance(dest_val, list):
                    updated_list = _deep_update_list(source_val)
                    setattr(dest, name, updated_list)
                elif isinstance(source_val, set) and isinstance(dest_val, set):
                    updated_set = _deep_update_set(source_val)
                    setattr(dest, name, updated_set)
                elif isinstance(source_val, tuple) and isinstance(dest_val, tuple):
                    updated_tuple = _deep_update_tuple(source_val)
                    setattr(dest, name, updated_tuple)
                else:
                    # For non-containers, non-dataclasses, just copy the value
                    setattr(dest, name, copy.deepcopy(source_val))

        def _deep_update_dict(source_dict):
            """Update dictionary values, handling nested dataclasses."""
            result = {}
            for k, v in source_dict.items():
                if is_dataclass(v):
                    # For dataclass values, create a new instance
                    result[copy.deepcopy(k)] = self._copy_dataclass_instance(v)
                elif isinstance(v, dict):
                    result[copy.deepcopy(k)] = _deep_update_dict(v)
                elif isinstance(v, list):
                    result[copy.deepcopy(k)] = _deep_update_list(v)
                elif isinstance(v, set):
                    result[copy.deepcopy(k)] = _deep_update_set(v)
                elif isinstance(v, tuple):
                    result[copy.deepcopy(k)] = _deep_update_tuple(v)
                else:
                    result[copy.deepcopy(k)] = copy.deepcopy(v)
            return result

        def _deep_update_list(source_list):
            """Update list values, handling nested dataclasses."""
            return [
                (
                    self._copy_dataclass_instance(item)
                    if is_dataclass(item)
                    else (
                        _deep_update_dict(item)
                        if isinstance(item, dict)
                        else (
                            _deep_update_list(item)
                            if isinstance(item, list)
                            else (
                                _deep_update_set(item)
                                if isinstance(item, set)
                                else (
                                    _deep_update_tuple(item)
                                    if isinstance(item, tuple)
                                    else copy.deepcopy(item)
                                )
                            )
                        )
                    )
                )
                for item in source_list
            ]

        def _deep_update_set(source_set):
            """Update set values, handling nested dataclasses."""
            return {
                (
                    self._copy_dataclass_instance(item)
                    if is_dataclass(item)
                    else (
                        _deep_update_dict(item)
                        if isinstance(item, dict)
                        else (
                            _deep_update_list(item)
                            if isinstance(item, list)
                            else (
                                _deep_update_set(item)
                                if isinstance(item, set)
                                else (
                                    _deep_update_tuple(item)
                                    if isinstance(item, tuple)
                                    else copy.deepcopy(item)
                                )
                            )
                        )
                    )
                )
                for item in source_set
            }

        def _deep_update_tuple(source_tuple):
            """Update tuple values, handling nested dataclasses."""
            return tuple(
                (
                    self._copy_dataclass_instance(item)
                    if is_dataclass(item)
                    else (
                        _deep_update_dict(item)
                        if isinstance(item, dict)
                        else (
                            _deep_update_list(item)
                            if isinstance(item, list)
                            else (
                                _deep_update_set(item)
                                if isinstance(item, set)
                                else (
                                    _deep_update_tuple(item)
                                    if isinstance(item, tuple)
                                    else copy.deepcopy(item)
                                )
                            )
                        )
                    )
                )
                for item in source_tuple
            )

        # Verify dest_obj is of child type
        if not isinstance(dest_obj, self.child_cls):
            raise TypeError(f"dest_obj must be an instance of {self.child_cls}")

        # Update the destination object in-place
        _update_dataclass(source_obj, dest_obj)

    def compare(self, parent_obj, child_obj) -> bool:
        """
        Return True if all non-excluded fields in child_obj have the same
        value as in parent_obj.
        Collections are deeply compared, skipping any excluded fields at any level.
        Excluded properties (marked with @exclude) are skipped in comparison.
        """

        def _compare_values(parent_val, child_val, parent_obj=None, field_name=None):
            """Recursively compare values, handling dataclasses and containers.
            For dataclasses, skip excluded fields. For containers, compare elements.
            """
            # If comparing a field that's excluded on the parent, skip comparison
            if parent_obj is not None and field_name is not None:
                if self._is_excluded(parent_obj, field_name):
                    return True

            # Handle None values
            if parent_val is None and child_val is None:
                return True
            if parent_val is None or child_val is None:
                return False

            # If both are dataclasses, compare field by field
            if is_dataclass(parent_val) and is_dataclass(child_val):
                for f in fields(parent_val):
                    # Skip excluded fields in nested dataclasses
                    if self._is_excluded(parent_val, f.name):
                        continue

                    if not hasattr(parent_val, f.name) or not hasattr(
                        child_val, f.name
                    ):
                        return False

                    p_val = getattr(parent_val, f.name)
                    c_val = getattr(child_val, f.name)

                    if not _compare_values(p_val, c_val, parent_val, f.name):
                        return False
                return True

            # Handle containers recursively
            if isinstance(parent_val, dict) and isinstance(child_val, dict):
                if set(parent_val.keys()) != set(child_val.keys()):
                    return False
                return all(
                    _compare_values(parent_val[k], child_val[k]) for k in parent_val
                )

            if isinstance(parent_val, (list, tuple)) and isinstance(
                child_val, (list, tuple)
            ):
                if len(parent_val) != len(child_val):
                    return False
                return all(_compare_values(p, c) for p, c in zip(parent_val, child_val))

            if isinstance(parent_val, set) and isinstance(child_val, set):
                # For sets, we need to compare elements carefully since they may contain
                # dataclasses with excluded fields
                if len(parent_val) != len(child_val):
                    return False
                # Try to match each parent item with a child item
                remaining_child = list(child_val)
                for p in parent_val:
                    found = False
                    for i, c in enumerate(remaining_child):
                        if _compare_values(p, c):
                            remaining_child.pop(i)
                            found = True
                            break
                    if not found:
                        return False
                return True

            # For non-container, non-dataclass values, use regular equality
            return parent_val == child_val

        # Compare top-level fields
        for f in self._child_fields:
            if not hasattr(parent_obj, f.name) or not hasattr(child_obj, f.name):
                continue

            # Get values but delegate exclusion check to _compare_values
            parent_val = getattr(parent_obj, f.name)
            child_val = getattr(child_obj, f.name)

            if not _compare_values(parent_val, child_val, parent_obj, f.name):
                return False

        return True
