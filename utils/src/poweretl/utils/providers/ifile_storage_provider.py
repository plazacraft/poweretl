from abc import ABC, abstractmethod


class IFileStorageReader(ABC):

    @abstractmethod
    def get_first_file_or_folder(self, path: str, ascending: bool) -> str:
        pass


    @abstractmethod
    def get_folders_list(self, path: str, recursive: bool = False) -> list[str]:
        pass

    @abstractmethod
    def get_files_list(self, path: str, recursive: bool = False) -> list[str]:
        pass

    @abstractmethod
    def get_file_str_content(self, full_path: str) -> str:
        pass


class IFileStorageWriter(IFileStorageReader):

    @abstractmethod
    def upload_file_str(self, full_path: str, file: str, content: str):
        pass
