import json
from deepmerge import Merger
from dacite import from_dict
from dataclasses import asdict
from .BaseFileMerger import BaseFileMerger


class JsonFileMerger(BaseFileMerger):
    """ JSON merger.
    """
    def __init__(self):
        super().__init__()
        
    def _v_to_dict(self, content) -> dict:
        return json.loads(content)

