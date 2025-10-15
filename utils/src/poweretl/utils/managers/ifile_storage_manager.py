from abc import abstractmethod

from poweretl.utils.providers import IFileStorageProvider


class IFileStorageManager(IFileStorageProvider):

    @abstractmethod
    def upload_file_str(self, path: str, file: str, content: str):
        pass
