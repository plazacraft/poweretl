import copy
import uuid
from typing import Optional, Type, TypeVar

from poweretl.defs import IMetaProvider, Meta, Model
from poweretl.defs.meta import Column, Operation, Status, Table
from poweretl.utils import DataclassUpgrader

T = TypeVar("T")


class BaseMetaProvider(IMetaProvider):
    """Keeps Model metadata and it's provisioning state in file."""

    def __init__(self):
        pass

    def _v_create_or_update(
        self,
        source_obj,
        dest_obj: Optional[T],
        child_cls: Type[T],
    ) -> T:
        """
        Generic create-or-update for parent/child dataclasses.

        - If dest_obj exists and differs (excluding collections),
            return a new child with UPDATED status.
        - If dest_obj does not exist, return a new child with NEW status.
        - If dest_obj exists and is the same, return dest_obj unchanged.
        """
        upgrader = DataclassUpgrader(child_cls)

        if dest_obj:
            if not upgrader.are_the_same(source_obj, dest_obj):
                new_child = upgrader.from_parent(source_obj)
                new_child.meta.object_id = dest_obj.meta.object_id
                new_child.meta.operation = Operation.UPDATED.value
                new_child.meta.status = Status.PENDING.value
                return new_child
            # if no properties has been changed, we return object as it is
            return dest_obj

        new_child = upgrader.from_parent(source_obj)
        new_child.meta.object_id = str(uuid.uuid4())
        new_child.meta.operation = Operation.NEW.value
        new_child.meta.status = Status.PENDING.value
        return new_child

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

        for table_id, table in model.tables.items():

            # update table properties in meta or create new object
            dest_table = None
            if table_id in meta.tables.keys():
                dest_table = meta.tables[table_id]

            meta_table: Table = self._v_create_or_update(
                table,
                dest_table,
                Table,
            )

            for column_id, column in table.columns.items():
                # update column properties in meta or create new object
                dest_column = None
                if column_id in meta_table.columns.keys():
                    dest_column = meta_table.columns[column_id]

                meta_column = self._v_create_or_update(
                    column,
                    dest_column,
                    Column,
                )

                meta_table.columns[column_id] = meta_column

            meta.tables[table_id] = meta_table

            # mark not existed columns to remove
            if table.prune_columns:
                for (
                    meta_current_column_id,
                    meta_current_column,
                ) in meta_table.columns.items():
                    if meta_current_column_id not in table.columns.keys():
                        meta_current_column.meta.status = Status.PENDING.value
                        meta_current_column.meta.operation = Operation.DELETED.value

        # mark not existed tables to remove
        if model.prune_tables:
            for meta_current_table_id, meta_current_table in meta.tables.items():
                if meta_current_table_id not in model.tables.keys():
                    meta_current_table.meta.status = Status.PENDING.value
                    meta_current_table.meta.operation = Operation.DELETED.value

        return meta
