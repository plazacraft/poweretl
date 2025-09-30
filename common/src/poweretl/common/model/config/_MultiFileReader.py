
class _MultiFileReader():
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

    def get_names(self) -> list[str]:
        return None
    
    def get_files(self) -> dict[str, str]:
        return None

