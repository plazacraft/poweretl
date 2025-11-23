# pylint: disable=R0914, W0511, R0912, W0718

import copy
import uuid
from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import Any, Optional, TypeVar, Union, cast, get_type_hints

from deepdiff import DeepDiff
from poweretl.defs.meta import BaseCollection, BaseItem, Meta, Operation, Status, Table
from poweretl.defs.model import BaseCollection as ModelBaseCollection
from poweretl.defs.model import BaseItem as ModelBaseItem
from poweretl.defs.model import Model
from poweretl.defs.model import Table as ModelTable
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
            updated_fields = {}
            for f in fields(dst):
                # if there is no field in model, new value is None, old is from meta
                dst_attr = getattr(dst, f.name)
                if not src:
                    updated_fields[f.name] = dst_attr
                    continue

                if hasattr(src, f.name):
                    src_attr = getattr(src, f.name)
                    # diff only attributes that are copied by upgrader
                    if not isinstance(dst_attr, (BaseItem, BaseCollection)):
                        diff = DeepDiff(src_attr, dst_attr)
                        if diff:
                            updated_fields[f.name] = dst_attr
            return updated_fields

        def _merge_updated_fields(src, to_merge):
            for key, value in to_merge.items():
                if key in src:
                    src[key] = value

        upgrader = DataclassUpgrader(child_cls)

        if dest_obj:  # pylint: disable=R1702

            updated_fields = {}
            updated = False
            if not upgrader.compare(source_obj, dest_obj):
                updated_fields = _get_updated_fields(source_obj, dest_obj)
                upgrader.update_child(source_obj, dest_obj)

            # Define the action base on the previous status

            if dest_obj.meta.operation == Operation.DELETED.value:
                # if it was not removed then it will be updated,
                # if it was successfully
                # removed then it will be added as new
                if dest_obj.meta.status != Status.SUCCESS.value or dest_obj.linked:
                    dest_obj.meta.operation = Operation.UPDATED.value
                    dest_obj.meta.updated_fields = updated_fields
                else:
                    dest_obj.meta.operation = Operation.NEW.value
                updated = True
            elif dest_obj.meta.operation == Operation.NEW.value:
                # if previous status was New but not success it should stays as new,
                # it should change status only if was success
                if dest_obj.meta.status == Status.SUCCESS.value:
                    dest_obj.meta.operation = Operation.UPDATED.value
                    updated = True

            elif dest_obj.meta.operation == Operation.UPDATED.value and updated_fields:
                # for update we change only if there are updated fields,
                # for not success we need to merge not updated fields
                if dest_obj.meta.status == Status.SUCCESS.value:
                    dest_obj.meta.updated_fields = updated_fields
                else:
                    _merge_updated_fields(dest_obj.meta.updated_fields, updated_fields)
                updated = True

            if updated:
                dest_obj.meta.status = Status.PENDING.value
                dest_obj.meta.model_last_update = datetime.now().isoformat()

            return dest_obj

        new_child = upgrader.from_parent(source_obj)
        new_child.meta.object_id = str(uuid.uuid4())

        # for linked item is not created, however all fields are updated
        if new_child.linked:
            new_child.meta.operation = Operation.UPDATED.value
            new_child.meta.updated_fields = _get_updated_fields(None, new_child)
        else:
            new_child.meta.operation = Operation.NEW.value
        new_child.meta.status = Status.PENDING.value
        new_child.meta.model_last_update = datetime.now().isoformat()
        return new_child

    def _set_deletion_status(self, current_item: BaseItem, operation, status):
        current_item.meta.operation = operation
        current_item.meta.status = status
        current_item.meta.model_last_update = datetime.now().isoformat()

        for f in fields(current_item):
            if isinstance(getattr(current_item, f.name), ModelBaseCollection):
                collection_item: ModelBaseCollection = getattr(current_item, f.name)
                for inner_item in collection_item.items.values():
                    self._set_deletion_status(inner_item, operation, status)

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
                # if item is already deleted, then it's status
                # shouldn't be changed for double deletion
                if (
                    (meta_current_item_id not in model_collection.items.keys())
                    and (not meta_current_item.linked)
                    and (meta_current_item.meta.operation != Operation.DELETED.value)
                ):

                    # if item was new but not provisioned correctly,
                    # then set it automatically to Deleted
                    if (
                        meta_current_item.meta.operation == Operation.NEW.value
                        and meta_current_item.meta.status != Status.SUCCESS.value
                    ):
                        new_status = Status.SUCCESS.value
                    else:
                        new_status = Status.PENDING.value

                    new_operation = Operation.DELETED.value
                    self._set_deletion_status(
                        meta_current_item, new_operation, new_status
                    )

    def _update_synced_meta(self, meta_item: BaseItem, model_item: ModelBaseItem):
        if not model_item:
            self._set_deletion_status(
                meta_item, Operation.DELETED.value, Status.SUCCESS.value
            )
        else:
            upgrader = DataclassUpgrader(type(meta_item))
            preserve_linked = meta_item.linked
            upgrader.update_child(model_item, meta_item)
            meta_item.linked = preserve_linked

            for f in fields(meta_item):
                if hasattr(model_item, f.name):
                    meta_attr = getattr(meta_item, f.name)
                    model_attr = getattr(model_item, f.name)
                    if isinstance(meta_attr, BaseCollection):
                        meta_collection: BaseCollection = meta_attr
                        model_collection: ModelBaseCollection = model_attr
                        for item in meta_collection.items.values():

                            # match model item by name
                            model_matched_item = next(
                                (
                                    model_item
                                    for model_item in model_collection.items.values()
                                    if model_item.name == item.name
                                ),
                                None,
                            )
                            self._update_synced_meta(item, model_matched_item)

            meta_item.meta.status = Status.SUCCESS.value
            meta_item.meta.operation = Operation.UPDATED.value
            meta_item.meta.model_last_update = datetime.now().isoformat()

        meta_item.meta.error_msg = None

    def get_synced_meta(self, model_table: ModelTable, meta_table: Table) -> Table:
        table_copy = copy.deepcopy(meta_table)
        self._update_synced_meta(table_copy, model_table)
        return table_copy

    def get_updated_meta(self, model: Model, meta: Meta) -> Meta:
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

    def _apply_status_filter(self, meta, statuses: set[str]):
        """Recursively filter object and all its collections by status.

        Args:
            obj: Object to filter
            statuses: Statuses to filter by

        Returns:
            Filtered object with only matching status branches
        """

        child_included = False
        meta_copy = type(meta)()

        for field in fields(meta):
            obj = getattr(meta, field.name)

            if isinstance(obj, BaseItem):
                item = cast(BaseItem, obj)
                attr, current_child_included = self._apply_status_filter(item, statuses)
                if attr and current_child_included:
                    setattr(meta_copy, field.name, attr)
                    child_included = True

            elif isinstance(obj, BaseCollection):
                attr, current_child_included = self._apply_status_filter(obj, statuses)
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
                        obj_item, statuses
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
            if not child_included and item.meta.status not in statuses:
                return None, False
            if item.meta.status in statuses:
                return meta_copy, True

        return meta_copy, child_included

    def apply_status_filter(self, meta, statuses: set[str]):
        ret, _ = self._apply_status_filter(meta, statuses)
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
