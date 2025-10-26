# pylint: disable=R0912

import os
from pathlib import Path

from poweretl.defs import IMetaProvider, IModelManager
from poweretl.defs.meta import Operation, Status
from poweretl.utils import FileEntry, FileMerger, MultiFileReader


class BaseModelManager(IModelManager):

    def __init__(
        self,
        meta_provider: IMetaProvider,
        config_paths: list[FileEntry] = [
            FileEntry(
                Path(os.path.dirname(os.path.abspath(__file__))).joinpath("_data"),
                r"sql_model_manager\.yaml",
            )
        ],
        encoding: str = "utf-8",
    ):
        self._meta_provider = meta_provider
        self._config_reader = MultiFileReader(
            file_paths=config_paths, encoding=encoding
        )
        self._file_merger = FileMerger()

    def _v_create_table(self):
        pass

    def _v_alter_table_rename(self):
        pass

    def _v_alter_table_comment(self):
        pass

    def provision_model(self):
        meta = self._meta_provider.get_meta(status=Status.PENDING.value)
        for table in meta.tables.items.values():
            if table.meta.operation == Operation.NEW:
                pass
            elif table.meta.operation == Operation.UPDATED:
                pass
            elif table.meta.operation == Operation.DELETED:
                pass

            for column in table.columns.items.values():
                if column.meta.operation == Operation.NEW:
                    pass
                elif column.meta.operation == Operation.UPDATED:
                    pass
                elif column.meta.operation == Operation.DELETED:
                    pass
                for column_tag in column.tags.items.values():
                    if column_tag.meta.operation == Operation.NEW:
                        pass
                    elif column_tag.meta.operation == Operation.UPDATED:
                        pass
                    elif column_tag.meta.operation == Operation.DELETED:
                        pass

            for tag in table.tags.items.values():
                if tag.meta.operation == Operation.NEW:
                    pass
                elif tag.meta.operation == Operation.UPDATED:
                    pass
                elif tag.meta.operation == Operation.DELETED:
                    pass

            for current_property in table.properties.items.values():
                if current_property.meta.operation == Operation.NEW:
                    pass
                elif current_property.meta.operation == Operation.UPDATED:
                    pass
                elif current_property.meta.operation == Operation.DELETED:
                    pass


# from poweretl.utils import MemFileStorageProvider

# from poweretl.common.providers.file_meta_provider import FileMetaProvider

# meta_provider = FileMetaProvider(storage_provider=MemFileStorageProvider())
# mgr = BaseModelManager(meta_provider)
