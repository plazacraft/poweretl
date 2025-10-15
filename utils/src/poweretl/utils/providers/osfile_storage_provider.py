from pathlib import Path

from .ifile_storage_provider import IFileStorageProvider


class OSFileStorageProvider(IFileStorageProvider):

    def __init__(self):
        pass

    def get_files_list(self, path: str, recursive: bool) -> list[str]:
        root = Path(path)
        files = []
        if recursive:
            files = root.rglob("*")
        else:
            files = root.iterdir()

        all_files = [file.resolve() for file in files if file.is_file()]
        return all_files

    def get_file_str_content(self, full_path: str, encoding: str) -> str:
        with open(full_path, "r", encoding=encoding) as f:
            return f.read()
