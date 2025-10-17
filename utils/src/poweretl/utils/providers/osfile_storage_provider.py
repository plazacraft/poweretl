from pathlib import Path

from .ifile_storage_provider import IFileStorageWriter


class OSFileStorageProvider(IFileStorageWriter):

    def __init__(self, encoding: str = "utf-8"):
        self._encoding = encoding

    def get_first_file_or_folder(
        self, path: str, ascending: bool = True
    ) -> tuple[str, bool]:
        root = Path(path)
        files = root.iterdir()
        if files:
            files = sorted(
                files, key=lambda f: (f.name.lower()), reverse=(not ascending)
            )

            return (files[0].absolute(), files[0].is_dir())

        return None

    def get_folders_list(self, path: str, recursive: bool = False) -> list[str]:
        root = Path(path)
        folders = []
        if recursive:
            folders = root.rglob("*")
        else:
            folders = root.iterdir()

        all_folders = [folder.resolve() for folder in folders if folder.is_dir()]
        return all_folders

    def get_files_list(self, path: str, recursive: bool = False) -> list[str]:
        root = Path(path)
        files = []
        if recursive:
            files = root.rglob("*")
        else:
            files = root.iterdir()

        all_files = [file.resolve() for file in files if file.is_file()]
        return all_files

    def get_file_str_content(self, full_path: str) -> str:
        with open(full_path, "r", encoding=self._encoding) as f:
            return f.read()

    def upload_file_str(self, full_path: str, content: str):

        with open(full_path, "w+", encoding=self._encoding) as f:
            f.write(content)
