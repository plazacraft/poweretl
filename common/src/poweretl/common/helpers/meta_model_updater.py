# pylint: disable=R0914

import copy
from dataclasses import fields
from typing import Optional, TypeVar, get_type_hints

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
