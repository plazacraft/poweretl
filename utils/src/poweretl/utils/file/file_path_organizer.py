import re
from datetime import datetime


class FilePathOrganizer:
    """Just returns paths and file names based on pattern and target timestamp

    Attributes:
        path_pattern (str): Use
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
        file_patter (str): Use
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    """

    def __init__(self, 
                 path_pattern: str = "%Y-%m/%d", 
                 file_pattern: str = "%Y-%m-%d_%H-%M-%S_%f"):
        self.path_pattern = path_pattern
        self.file_pattern = file_pattern

    def _sanitize(self, name: str) -> str:
        return re.sub(r'[:*?"<>|]', "", name)

    def get_name(self, timestamp: datetime):
        folder = timestamp.strftime(self.path_pattern)
        file_prefix = timestamp.strftime(self.file_pattern)

        folder = self._sanitize(folder)
        file_prefix = self._sanitize(file_prefix)

        return folder, file_prefix
