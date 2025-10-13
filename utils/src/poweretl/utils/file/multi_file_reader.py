import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FileEntry:
    path: str
    regex: str
    recursive: bool = True


class MultiFileReader:
    """Initializes the _MultiFileReader.
    Attributes:
        file_paths (list[FileEntry]): List of
            FileEntry objects containing paths and regex patterns.
        encoding (str, optional): Encoding to use for reading files.
            Defaults to 'utf-8'.
    """

    def __init__(self, file_paths: list[FileEntry], encoding: str = "utf-8"):
        self._file_paths = file_paths
        self._encoding = encoding

    def get_files(self) -> list[Path]:
        output = []
        if self._file_paths:
            for file_entry in self._file_paths:
                root = Path(file_entry.path)
                regex = re.compile(file_entry.regex)
                if file_entry.recursive:
                    files = root.rglob("*")
                else:
                    files = root.iterdir()

                all_files = [
                    file for file in files if file.is_file() and regex.search(file.name)
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
        return output

    def get_files_with_content(self) -> list[tuple[Path, str]]:
        output = []
        files = self.get_files()
        for file in files:
            with open(file, "r", encoding=self._encoding) as f:
                output.append((file, f.read()))

        return output
