import yaml
from deepmerge import Merger
from .BaseFileMerger import BaseFileMerger

class YamlFileMerger(BaseFileMerger):
    """YAML merger."""
    def __init__(self):
        super().__init__()

    def _v_to_dict(self, content) -> dict:
        return yaml.safe_load(content)

