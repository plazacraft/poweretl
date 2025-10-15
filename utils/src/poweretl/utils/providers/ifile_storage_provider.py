from abc import ABC, abstractmethod


class IFileStorageReader(ABC):

    @abstractmethod
    def get_files_list(self, path: str, recursive: bool) -> list[str]:
        pass

    def get_file_str_content(self, full_path: str, encoding: str) -> str:
        pass


class IFileStorageWriter(ABC):

    @abstractmethod
    def upload_file_str(self, path: str, file: str, content: str):
        pass
