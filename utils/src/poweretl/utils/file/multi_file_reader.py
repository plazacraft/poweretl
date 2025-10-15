import re
from dataclasses import dataclass
from pathlib import Path

from poweretl.utils.providers import IFileStorageProvider, OSFileStorageProvider


@dataclass
class FileEntry:
    path: str
    regex: str
    recursive: bool = True


class MultiFileReader:
    """Reads file paths and its contents based on provided regex patterns.
    Attributes:
        file_paths (list[FileEntry]): List of
            FileEntry objects containing paths and regex patterns.
        encoding (str, optional): Encoding to use for reading files.
            Defaults to 'utf-8'.
    """

    def __init__(
        self,
        file_paths: list[FileEntry],
        encoding: str = "utf-8",
        file_storage_provider: IFileStorageProvider = OSFileStorageProvider(),
    ):
        self._file_paths = file_paths
        self._encoding = encoding
        self._file_storage_provider = file_storage_provider

    def get_files(self) -> list[str]:
        """Get file paths.

        Returns:
            list[Path]: Ordered list of file paths.
        """
        output = []
        if self._file_paths:
            for file_entry in self._file_paths:
                regex = re.compile(file_entry.regex)
                file_list = self._file_storage_provider.get_files_list(
                    file_entry.path, file_entry.recursive
                )
                file_list_paths = [Path(file) for file in file_list]
                all_files = [
                    file
                    for file in file_list_paths
                    if file.is_file() and regex.search(file.name)
                ]

                # Sort by parent folder full path and name, then by file name
                sorted_files = sorted(
                    all_files,
                    key=lambda f: (
                        str(f.parent.resolve()),
                        f.parent.name.lower(),
                        f.name.lower(),
                    ),
                )
                output.extend(sorted_files)
        output = [file_path.resolve() for file_path in output]
        return output

    def get_files_with_content(self) -> list[tuple[str, str]]:
        """Get files and their contents.

        Returns:
            list[tuple[Path, str]]: Ordered list of pair Path-Content.
        """
        output = []
        files = self.get_files()
        for file in files:
            output.append(
                (
                    file,
                    self._file_storage_provider.get_file_str_content(
                        file, self._encoding
                    ),
                )
            )
        return output
