import json
from deepmerge import Merger
from dacite import from_dict
from dataclasses import asdict


class JsonFileMerger:
    """ JSON merger.
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

    def merge(self, contents: list[str]) -> dict:
        data = None
        for content in contents:
            json_data = None
            if content:
                json_data = json.loads(content)

            if json_data:
                if data is None:
                    data = json_data
                else:
                    self._merger.merge(data, json_data)
        return data
