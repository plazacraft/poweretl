from pathlib import Path
from deepmerge import always_merger
from dacite import from_dict
from datetime import datetime
from dataclasses import asdict

from poweretl.defs import IMetaProvider, Meta, Model
from poweretl.utils import IFileStorageWriter, OSFileStorageProvider, FileSerializer, FilePathOrganizer

class FileMetaProvider(IMetaProvider):
    """Keeps Model metadata and it's provisioning state in file."""

    def __init__(self, 
                 file_name: str = "metadata.json",
                 path: str = ".", 
                 store_versions: bool = False,
                 storage_provider: IFileStorageWriter = OSFileStorageProvider(),
                 file_serializer: FileSerializer = FileSerializer(),
                 file_path_organizer: FilePathOrganizer = FilePathOrganizer()
                 ):
        self._store_versions = store_versions
        self._storage_provider = storage_provider
        self._file_serializer = file_serializer
        self._file_path_organizer = file_path_organizer
        self._merger = always_merger
        self._path = path
        self._file_name = file_name

    
    def _find_latest_file(self, path: str):

        if (not self._store_versions):
            return self._file_name
        else:
            item = self._storage_provider.get_first_file_or_folder(self._path, False)
            if (not item):
                return None
            if (item[1] == True):
                return self._find_latest_file(item[0])
            else:
                return item[0]
            
    def _save_meta(self, meta: Meta):
        output_file = None
        output_dir = ""
        if (not self._store_versions):
            output_file = self._file_name
        else:
            output_dir, output_file = self._file_path_organizer.get_name(datetime.now())
            output_file = Path(output_file).joinpath(self._file_name)
            
        output_dir = Path(self._path).joinpath(output_dir)
        self._file_serializer.to_file_content(output_file, asdict(meta))


    def update_self_model(self):
        return

    # Model vs Meta
    # detect what is new, what to delete and what to remove
    # this function can be generic!
    def push_model_changes(self, model: Model):
        pass

    def push_meta_changes(self, meta: Meta):
        meta_current_model = self.get_model_meta()
        always_merger.merge(meta_current_model, meta)


        self._storage_provider.upload_file_str()            
        pass

    def get_model_meta(self, table_id: str = None) -> Meta:
        file = self._find_latest_file(self._path)
        if (file):
            content = self._storage_provider.get_file_str_content(file)
            if (content):
                meta_dict = self._file_serializer.to_dict(Path(file).name, content)
                meta = from_dict(data_class=Meta, data=meta_dict)

                if (table_id and meta):
                    meta.tables = [table for table in meta.tables if table["id"] == table_id]
                return meta

        return None

