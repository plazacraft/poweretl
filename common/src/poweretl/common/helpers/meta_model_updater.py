# pylint: disable=R0914

import copy
from dataclasses import fields
from typing import Optional, TypeVar, get_type_hints, cast

from poweretl.defs.meta import BaseCollection, BaseItem, Operation, Status
from poweretl.defs.model import BaseCollection as ModelBaseCollection
from poweretl.utils import DataclassUpgrader

BaseItemT = TypeVar("BaseItemT", bound=BaseItem)
BaseCollectionT = TypeVar("BaseCollectionT", bound=BaseCollection)
ModelBaseCollectionT = TypeVar("ModelBaseCollectionT", bound=ModelBaseCollection)


class MetaModelUpdater:
    """Helper class extracted from BaseMetaProvider to perform create/update
    operations on meta-model objects.
    """

    def __init__(self):
        pass

    def _v_create_or_update(
        self,
        source_obj,
        dest_obj: Optional[BaseItem],
        child_cls: type,
    ) -> BaseItem:
        """Generic create-or-update for parent/child dataclasses.

        - If dest_obj exists and differs (excluding collections), update it in-place
          and mark UPDATED/PENDING.
        - If dest_obj does not exist, create a new child, mark NEW/PENDING
            and return it.
        - If dest_obj exists and is the same, return dest_obj unchanged.
        """
        upgrader = DataclassUpgrader(child_cls)

        if dest_obj:
            if not upgrader.compare(source_obj, dest_obj):
                upgrader.update_child(source_obj, dest_obj)
                dest_obj.meta.operation = Operation.UPDATED.value
                dest_obj.meta.status = Status.PENDING.value
                return dest_obj
            return dest_obj

        new_child = upgrader.from_parent(source_obj)
        new_child.meta.object_id = (
            str(copy.deepcopy(getattr(new_child.meta, "object_id", ""))) or None
        )
        # if meta.object_id should be generated here, set uuid when caller expects
        new_child.meta.operation = Operation.NEW.value
        new_child.meta.status = Status.PENDING.value
        return new_child

    def _v_update_collection(
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

            meta_item: BaseItem = self._v_create_or_update(
                item,
                dest_item,
                value_type,
            )

            for f in fields(item):
                if isinstance(getattr(item, f.name), ModelBaseCollection):
                    self._v_update_collection(
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
        """Return an updated Meta object based on provided model.

        This makes a deepcopy of meta, updates its fields from model while
        respecting excluded fields, and updates nested collections.
        """
        meta_copy = copy.deepcopy(meta)
        upgrader = DataclassUpgrader(type(meta))
        upgrader.update_child(model, meta_copy)

        for f in fields(model):
            if isinstance(getattr(model, f.name), ModelBaseCollection):
                self._v_update_collection(
                    getattr(meta_copy, f.name),
                    getattr(model, f.name),
                )

        return meta_copy





    def _apply_status_filter(self, meta, status: str):
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
                if (attr and current_child_included):
                    setattr(meta_copy, field.name, attr)
                    child_included = True
            
            elif isinstance(obj, BaseCollection):
                attr, current_child_included = self._apply_status_filter(obj, status)
                if (attr and current_child_included):
                    setattr(meta_copy, field.name, attr)
                    child_included = True

            elif isinstance(meta, BaseCollection) and field.name =="items" and isinstance(obj, dict):
                items = {}
                for obj_key, obj_item in obj.items():
                    attr, current_child_included = self._apply_status_filter(obj_item, status)
                    if (attr and current_child_included):
                        items[obj_key] = attr
                
                if (items):
                    setattr(meta_copy, field.name, items)
                    child_included = True

            else:
                setattr(meta_copy, field.name, copy.deepcopy(obj))
        
        if (isinstance(meta, BaseItem)):
            item = cast(BaseItem, meta)
            if (not child_included and item.meta.status != status):
                return None, False
            elif (item.meta.status == status):
                return meta_copy, True

        return meta_copy, child_included
            

    def apply_status_filter(self, meta, status: str):
        ret, _ = self._apply_status_filter(meta, status)
        return ret

