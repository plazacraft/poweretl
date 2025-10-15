from poweretl.utils.providers import OSFileStorageProvider

from .ifile_storage_manager import IFileStorageManager


class OSFileStorageManager(OSFileStorageProvider, IFileStorageManager):

    def __init__(self):
        pass

    def upload_file_str(self, path: str, file: str, content: str):
        pass
