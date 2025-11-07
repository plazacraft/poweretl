from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import TypeVar

from dacite import from_dict  # pylint: disable=C0411
from deepmerge import always_merger  # pylint: disable=C0411
from poweretl.defs import Meta, Model
from poweretl.defs.meta import BaseItem
from poweretl.utils import (
    DataclassUpgrader,
    FilePathOrganizer,
    FileSerializer,
    IFileStorageWriter,
    OSFileStorageProvider,
)

from .base_meta_provider import BaseMetaProvider

T = TypeVar("T")


class FileMetaProvider(BaseMetaProvider):
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
        super().__init__()
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
        self._storage_provider.upload_file_str(
            str(Path(output_dir).joinpath(output_file).as_posix()), content
        )

    def self_update(self):
        return

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

    def push_meta_item_changes(self, item: BaseItem):
        meta_current_model = self.get_meta()
        item_to_update = self._find_by_object_id(meta_current_model, item)
        upgrader = DataclassUpgrader(BaseItem)
        upgrader.update_child(item, item_to_update)
        self._save_meta(meta_current_model)

    def get_meta(self, status: set[str] = None, table_id: str = None) -> Meta:
        file = self._find_latest_file(self._path)
        if file:
            content = self._storage_provider.get_file_str_content(file)
            if content:
                meta_dict = self._file_serializer.to_dict(Path(file).name, content)
                meta = from_dict(data_class=Meta, data=meta_dict)

                if table_id and meta:
                    meta.tables.items = {
                        key: table
                        for key, table in meta.tables.items.items()
                        if key == table_id
                    }

                if status and meta:
                    meta = self._apply_status_filter(meta, status)

                return meta
        # new meta if there is no file found
        return Meta()
