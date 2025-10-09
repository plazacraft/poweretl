from abc import abstractmethod
import json
import yaml
from deepmerge import Merger
from dacite import from_dict
from dataclasses import asdict
from pathlib import Path


class FileMerger:

    SUPPORTED_EXTENSIONS = ['.json', '.yaml', '.yml']

    """ Merger.
    """
    def __init__(self):
        pass

    # default strategy of always_merger
    _merger = Merger(
        [
            (dict, "merge"), 
            (list, "append"), 
            (set, "union")
        ],
        ["override"],
        ["override"]
    )


    @abstractmethod
    def _to_dict(self, file: Path, content) -> dict:
        if (not file.is_file()):
            return None
        
        ext = file.suffix
        if ext == '.json':
            return json.loads(content)
        elif ext in ['.yaml', '.yml']:
            return yaml.safe_load(content)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
        return None

    def merge(self, files: list[tuple[Path,str]]) -> dict:
        data = None
        for file, content in files:
            file_data = None
            if content:
                file_data = self._to_dict(file, content)

            if file_data:
                if data is None:
                    data = file_data
                else:
                    self._merger.merge(data, file_data)
        return data
