# pylint: disable=R0914, W0511, R0912, W0718

import copy
from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import Any, Optional, TypeVar, Union, cast, get_type_hints

from deepdiff import DeepDiff
from poweretl.defs.meta import BaseCollection, BaseItem, Operation, Status
from poweretl.defs.model import BaseCollection as ModelBaseCollection
from poweretl.utils import DataclassUpgrader

BaseItemT = TypeVar("BaseItemT", bound=BaseItem)
# BaseCollectionT = TypeVar("BaseCollectionT", bound=BaseCollection)
# ModelBaseCollectionT = TypeVar("ModelBaseCollectionT", bound=ModelBaseCollection)


class MetaModelUpdater:
    """Helper class extracted from BaseMetaProvider to perform create/update
    operations on meta-model objects.
    """

    def __init__(self):
        pass

    def _create_or_update(
        self,
        source_obj,
        dest_obj: Optional[BaseItem],
        child_cls: type[BaseItemT],
    ) -> BaseItem:
        """Generic create-or-update for parent/child dataclasses.

        - If dest_obj exists and differs (excluding collections), update it in-place
          and mark UPDATED/PENDING.
        - If dest_obj does not exist, create a new child, mark NEW/PENDING
            and return it.
        - If dest_obj exists and is the same, return dest_obj unchanged.
        """

        def _get_updated_fields(src, dst):
            updated_fields = []
            for f in fields(dst):
                if (not src):
                    updated_fields.append(f.name)
                    continue

                if hasattr(src, f.name):
                    src_attr = getattr(src, f.name)
                    dst_attr = getattr(dst, f.name)
                    # diff only attributes that are copied by upgrader
                    if not isinstance(dst_attr, (BaseItem, BaseCollection)):
                        diff = DeepDiff(src_attr, dst_attr)
                        if diff:
                            updated_fields.append(f.name)
            return updated_fields

        upgrader = DataclassUpgrader(child_cls)

        if dest_obj:  # pylint: disable=R1702
            if not upgrader.compare(source_obj, dest_obj):
                updated_fields = _get_updated_fields(source_obj, dest_obj)
                upgrader.update_child(source_obj, dest_obj)
                dest_obj.meta.operation = Operation.UPDATED.value
                dest_obj.meta.status = Status.PENDING.value
                dest_obj.meta.updated_fields = updated_fields
                dest_obj.meta.model_last_update = datetime.now().isoformat()
                return dest_obj
            return dest_obj

        new_child = upgrader.from_parent(source_obj)
        new_child.meta.object_id = (
            str(copy.deepcopy(getattr(new_child.meta, "object_id", ""))) or None
        )

        # for linked item is not created, however all fields are updated
        if (new_child.linked):
            new_child.meta.operation = Operation.UPDATED.value
            new_child.meta.updated_fields = _get_updated_fields(None, new_child)
        else:
            new_child.meta.operation = Operation.NEW.value
        new_child.meta.status = Status.PENDING.value
        new_child.meta.model_last_update = datetime.now().isoformat()
        return new_child

    def _update_collection(
        self, meta_collection: BaseCollection, model_collection: ModelBaseCollection
    ):
        """Synchronize items from model_collection into meta_collection.
        Creates/updates items and marks deletions when pruning.
        """
        for item_id, item in model_collection.items.items():
            dest_item = None
            if item_id in meta_collection.items.keys():
                dest_item = meta_collection.items[item_id]

            # Determine the value type for items (second generic arg of items)
            value_type = get_type_hints(meta_collection.__class__)["items"].__args__[1]

            meta_item: BaseItem = self._create_or_update(
                item,
                dest_item,
                value_type,
            )

            for f in fields(item):
                if isinstance(getattr(item, f.name), ModelBaseCollection):
                    self._update_collection(
                        getattr(meta_item, f.name),
                        getattr(item, f.name),
                    )

            meta_collection.items[item_id] = meta_item

        # mark not existed items to remove
        if getattr(model_collection, "prune", False):
            for meta_current_item_id, meta_current_item in list(
                meta_collection.items.items()
            ):
                if meta_current_item_id not in model_collection.items.keys():
                    meta_current_item.meta.status = Status.PENDING.value
                    meta_current_item.meta.operation = Operation.DELETED.value

    def get_updated_meta(self, model, meta):
        # TODO: This code doesn't assume BaseItem to be directly a child of BaseItem,
        # if such situation happens, it needs to be updated
        # TODO: Fields under items(prune) will be copied (not BaseItems) and will
        # change parent status to Updated if changed,
        # however not be in list of updated fields
        # on the other hand this behavior is good for prune

        """Return an updated Meta object based on provided model.

        This makes a deepcopy of meta, updates its fields from model while
        respecting excluded fields, and updates nested collections.
        """
        meta_copy = copy.deepcopy(meta)
        upgrader = DataclassUpgrader(type(meta))
        upgrader.update_child(model, meta_copy)

        for f in fields(model):
            if isinstance(getattr(model, f.name), ModelBaseCollection):
                self._update_collection(
                    getattr(meta_copy, f.name),
                    getattr(model, f.name),
                )

        return meta_copy

    def _apply_status_filter(self, meta, status: set[str]):
        """Recursively filter object and all its collections by status.

        Args:
            obj: Object to filter
            status: Status to filter by

        Returns:
            Filtered object with only matching status branches
        """

        child_included = False
        meta_copy = type(meta)()

        for field in fields(meta):
            obj = getattr(meta, field.name)

            if isinstance(obj, BaseItem):
                item = cast(BaseItem, obj)
                attr, current_child_included = self._apply_status_filter(item, status)
                if attr and current_child_included:
                    setattr(meta_copy, field.name, attr)
                    child_included = True

            elif isinstance(obj, BaseCollection):
                attr, current_child_included = self._apply_status_filter(obj, status)
                if attr and current_child_included:
                    setattr(meta_copy, field.name, attr)
                    child_included = True

            elif (
                isinstance(meta, BaseCollection)
                and field.name == "items"
                and isinstance(obj, dict)
            ):
                items = {}
                for obj_key, obj_item in obj.items():
                    attr, current_child_included = self._apply_status_filter(
                        obj_item, status
                    )
                    if attr and current_child_included:
                        items[obj_key] = attr

                if items:
                    setattr(meta_copy, field.name, items)
                    child_included = True

            else:
                setattr(meta_copy, field.name, copy.deepcopy(obj))

        if isinstance(meta, BaseItem):
            item = cast(BaseItem, meta)
            if not child_included and item.meta.status not in status:
                return None, False
            if item.meta.status in status:
                return meta_copy, True

        return meta_copy, child_included

    def apply_status_filter(self, meta, status: set[str]):
        ret, _ = self._apply_status_filter(meta, status)
        return ret

    def find_by_object_id(self, data: Union[dict, list, Any], target: Any):
        """
        Traverse nested dicts/lists/dataclasses looking for a dataclass
        instance whose .meta.object_id equals the provided target id.

        The `target` may be:
        - a dataclass (e.g. a BaseItem) in which case its `.meta.object_id`
          will be used as the search key, or
        - a scalar object_id (str/int) to search for directly.

        Returns the first matching dataclass instance found or None.
        Assumes at most one object will match the target id.
        """
        # normalize target to object_id value
        target_id = None
        try:
            # if target is a dataclass with `.meta.object_id`
            if is_dataclass(target) and hasattr(target, "meta"):
                target_id = getattr(getattr(target, "meta"), "object_id", None)
        except Exception:
            target_id = None

        if target_id is None:
            # fallback: target might already be an id
            target_id = target

        # internal recursive search
        def _search(node) -> Any:
            # dataclass instances: check .meta.object_id
            if is_dataclass(node):
                if hasattr(node, "meta") and hasattr(node.meta, "object_id"):
                    if node.meta.object_id == target_id:
                        return node
                # recurse into fields
                for f in fields(node):
                    try:
                        val = getattr(node, f.name)
                    except Exception:
                        continue
                    found = _search(val)
                    if found is not None:
                        return found

            # dict: recurse into values
            elif isinstance(node, dict):
                for v in node.values():
                    found = _search(v)
                    if found is not None:
                        return found

            # list/tuple: recurse into items
            elif isinstance(node, (list, tuple)):
                for it in node:
                    found = _search(it)
                    if found is not None:
                        return found

            return None

        return _search(data)
