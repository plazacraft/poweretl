

import os
from pathlib import Path
from poweretl.defs import IModelManager, Meta, IMetaProvider
from poweretl.utils import MultiFileReader, FileMerger, FileEntry

class BaseModelManager(IModelManager):

    def __init__(self, 
                    meta_provider: IMetaProvider,
                    config_paths: list[FileEntry] = [FileEntry(Path(os.path.dirname(os.path.abspath(__file__))).joinpath("_data"), r"sql_model_manager\.yaml")],
                    encoding: str = "utf-8",
                 ):
        self._meta_provider = meta_provider
        self._config_reader = MultiFileReader(
            file_paths=config_paths, encoding=encoding
        )
        self._file_merger = FileMerger()



    def _v_create_table():
        pass

    def _v_create_update_column():
        pass

    def provision_model(self):
        meta = self._meta_provider.get_meta()


from poweretl.common.providers.file_meta_provider import FileMetaProvider
from poweretl.utils import MemFileStorageProvider

meta_provider = FileMetaProvider(
    storage_provider=MemFileStorageProvider()
)
mgr = BaseModelManager(meta_provider)