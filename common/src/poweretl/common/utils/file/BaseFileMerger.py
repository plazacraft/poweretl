from abc import abstractmethod
import json
from deepmerge import Merger
from dacite import from_dict
from dataclasses import asdict


class BaseFileMerger:
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
    def _v_to_dict(self, content) -> dict:
        pass

    def merge(self, contents: list[str]) -> dict:
        data = None
        for content in contents:
            file_data = None
            if content:
                file_data = self._v_to_dict(content)

            if file_data:
                if data is None:
                    data = file_data
                else:
                    self._merger.merge(data, file_data)
        return data
