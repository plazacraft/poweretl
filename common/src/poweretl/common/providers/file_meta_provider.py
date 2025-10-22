import copy
import uuid
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Type, TypeVar

import poweretl.defs.meta as dmeta
from dacite import from_dict  # pylint: disable=C0411
from deepmerge import always_merger  # pylint: disable=C0411
from poweretl.defs import IMetaProvider, Meta, Model
from poweretl.utils import (
    DataclassUpgrader,
    FilePathOrganizer,
    FileSerializer,
    IFileStorageWriter,
    OSFileStorageProvider,
)

T = TypeVar("T")


class FileMetaProvider(IMetaProvider):
    """Keeps Model metadata and it's provisioning state in file."""

    def __init__(
        self,
        *,
        file_name: str = "metadata.json",
        path: str = ".",
        store_versions: bool = False,
        storage_provider: IFileStorageWriter = OSFileStorageProvider(),
        file_serializer: FileSerializer = FileSerializer(),
        file_path_organizer: FilePathOrganizer = FilePathOrganizer(),
    ):  # pylint: disable=R0913
        self._store_versions = store_versions
        self._storage_provider = storage_provider
        self._file_serializer = file_serializer
        self._file_path_organizer = file_path_organizer
        self._merger = always_merger
        self._path = path
        self._file_name = file_name

    def _find_latest_file(self, path: str):

        if not self._store_versions:
            return str(Path(path).joinpath(self._file_name))

        first, is_dir = self._storage_provider.get_first_file_or_folder(path, False)
        if not first:
            return None

        if is_dir:
            return self._find_latest_file(first)

        return first

    def _save_meta(self, meta: Meta):
        output_file = None
        output_dir = ""
        if not self._store_versions:
            output_file = self._file_name
        else:
            output_dir, output_file = self._file_path_organizer.get_name(datetime.now())
            output_file = output_file + self._file_name

        output_dir = str(Path(self._path).joinpath(output_dir))
        content = self._file_serializer.to_file_content(output_file, asdict(meta))
        self._storage_provider.upload_file_str(output_dir, output_file, content)

    def _create_or_update(
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
                new_child.meta.operation = dmeta.Operation.UPDATED
                new_child.meta.status = dmeta.Status.PENDING
                return new_child
            # if no properties has been changed, we return object as it is
            return dest_obj

        new_child = upgrader.from_parent(source_obj)
        new_child.meta.object_id = str(uuid.uuid4())
        new_child.meta.operation = dmeta.Operation.NEW
        new_child.meta.status = dmeta.Status.PENDING
        return new_child

    def update_self_model(self):
        return

    # Model vs Meta
    # detect what is new, what to delete and what to remove
    # this function can be generic!
    def _get_updated_meta(self, model: Model, meta: Meta) -> Meta:
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

            meta_table: dmeta.Table = self._create_or_update(
                table,
                dest_table,
                dmeta.Table,
            )

            for column_id, column in table.columns:
                # update column properties in meta or create new object
                dest_column = None
                if column_id in meta_table.columns.keys():
                    dest_column = meta_table.columns[column_id]

                meta_column = self._create_or_update(
                    column,
                    dest_column,
                    dmeta.Column,
                )

                meta_table.columns[column_id] = meta_column

            model.tables[table_id] = meta_table

            # mark not existed columns to remove
            if table.prune_columns:
                for (
                    meta_current_column_id,
                    meta_current_column,
                ) in meta_table.columns.items():
                    if meta_current_column_id not in table.columns.keys():
                        meta_current_column.meta.status = dmeta.Status.PENDING
                        meta_current_column.meta.operation = dmeta.Operation.DELETED

        # mark not existed tables to remove
        if model.prune_tables:
            for meta_current_table_id, meta_current_table in meta.tables.items():
                if meta_current_table_id not in model.tables.keys():
                    meta_current_table.meta.status = dmeta.Status.PENDING
                    meta_current_table.meta.operation = dmeta.Operation.DELETED

        return meta

    def push_model_changes(self, model: Model):
        # for file we take always whole file, but for database
        # it would be better to query by tables
        meta = self.get_meta()
        updated_meta = self._get_updated_meta(model, meta)
        self.push_meta_changes(updated_meta)

    def push_meta_changes(self, meta: Meta):
        meta_current_model = self.get_meta()
        meta_merged = always_merger.merge(meta_current_model, meta)
        self._save_meta(meta_merged)

    def get_meta(self, table_id: str = None) -> Meta:
        file = self._find_latest_file(self._path)
        if file:
            content = self._storage_provider.get_file_str_content(file)
            if content:
                meta_dict = self._file_serializer.to_dict(Path(file).name, content)
                meta = from_dict(data_class=Meta, data=meta_dict)

                if table_id and meta:
                    meta.tables = {
                        key: table
                        for key, table in meta.tables.items()
                        if key == table_id
                    }
                return meta

        return None
