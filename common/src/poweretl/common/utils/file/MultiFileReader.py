
import re
from pathlib import Path


class MultiFileReader():
    """Reads multiple files matching a regex pattern in ascending order.
    """
    def __init__(self, regex: str, file_paths: list[str], encoding: str = 'utf-8'):
        """Initializes the _MultiFileReader.
        Args:
            regex (str): Regular expression to match file names.
            file_paths (list[str]): Absolute paths to folder to scan for files.
            encoding (str, optional): Encoding to use for reading files. Defaults to 'utf-8'.
        """
        self._file_path = file_paths
        self._regex = regex
        self._encoding = encoding

    def get_files(self) -> list[Path]:
        output = []
        for file_path in self._file_path:
            root = Path(file_path)
            regex = re.compile(self._regex)

            all_files = [
                file for file in root.rglob("*")
                if file.is_file() and regex.search(file.name)
            ]

            # Sort by parent folder full path and name, then by file name
            sorted_files = sorted(
                all_files,
                key=lambda f: (str(f.parent.resolve()), f.parent.name.lower(), f.name.lower())
            )
            output.extend(sorted_files)
        return output

    def get_files_with_content(self) -> dict[Path, str]:
        output = {}
        files = self.get_files()
        for file in files:
            with open(file, 'r', encoding=self._encoding) as f:
                output[file] = f.read()
        
        # for dictionary order is not guaranteed
        return files, output

