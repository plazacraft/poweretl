# pylint: disable=R0914

import copy
import uuid
from typing import Container, Optional, Type, TypeVar, get_type_hints
from dataclasses import fields

from poweretl.defs import IMetaProvider, Meta, Model
from poweretl.defs.meta import BaseItem, Column, Operation, Status, Table, BaseCollection
from poweretl.defs.model import BaseCollection as ModelBaseCollection
from poweretl.utils import DataclassUpgrader
from tomlkit import table

BaseItemT = TypeVar("BaseItemT", bound=BaseItem)
BaseCollectionT = TypeVar("BaseCollectionT", bound=BaseCollection)
ModelBaseCollectionT = TypeVar("ModelBaseCollectionT", bound=ModelBaseCollection)

class BaseMetaProvider(IMetaProvider):
    """Keeps Model metadata and it's provisioning state in file."""

    def __init__(self):
        pass

    def _v_create_or_update(
        self,
        source_obj,
        dest_obj: Optional[BaseItemT],
        child_cls: Type[BaseItemT],
    ) -> BaseItemT:
        """
        Generic create-or-update for parent/child dataclasses.

        - If dest_obj exists and differs (excluding collections),
            return a new child with UPDATED status.
        - If dest_obj does not exist, return a new child with NEW status.
        - If dest_obj exists and is the same, return dest_obj unchanged.
        """
        upgrader = DataclassUpgrader(child_cls)

        if dest_obj:
            if not upgrader.compare(source_obj, dest_obj):
                upgrader.update_child(source_obj, dest_obj)
                dest_obj.meta.operation = Operation.UPDATED.value
                dest_obj.meta.status = Status.PENDING.value
                return dest_obj
            # if no properties has been changed, we return object as it is
            return dest_obj

        new_child = upgrader.from_parent(source_obj)
        new_child.meta.object_id = str(uuid.uuid4())
        new_child.meta.operation = Operation.NEW.value
        new_child.meta.status = Status.PENDING.value
        return new_child


    def _v_update_collection(
        self,
        meta_collection: Type[BaseCollectionT],
        model_collection: Type[ModelBaseCollectionT],
        ):

        for id, item in model_collection.items.items():

            # update table properties in meta or create new object
            dest_item = None
            if id in meta_collection.items.keys():
                dest_item = meta_collection.items[id]

            value_type = get_type_hints(meta_collection.__class__)['items'].__args__[1]


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


            meta_collection.items[id] = meta_item

        # mark not existed tables to remove
        if model_collection.prune:
            for meta_current_item_id, meta_current_item in meta_collection.items.items():
                if meta_current_item_id not in model_collection.items.keys():
                    meta_current_item.meta.status = Status.PENDING.value
                    meta_current_item.meta.operation = Operation.DELETED.value



    # Model vs Meta
    # detect what is new, what to delete and what to remove
    # this function can be generic!
    def _v_get_updated_meta(self, model: Model, meta: Meta) -> Meta:
        """_summary_

        Args:
            model (Model): _description_
            meta (Meta): _description_

        Returns:
            Meta: _description_
        """
        # don't update provided object, return new updated
        meta = copy.deepcopy(meta)
        upgrader = DataclassUpgrader(Meta)
        upgrader.update_child(model, meta)

        for f in fields(model):
            if isinstance(getattr(model, f.name), ModelBaseCollection):
                self._v_update_collection(
                    getattr(meta, f.name),
                    getattr(model, f.name),
                )    

        return meta
