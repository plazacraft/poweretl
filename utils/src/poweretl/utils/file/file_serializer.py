import json
from pathlib import Path
from enum import Enum

import json5
import yaml

class FileSerializer:
    """Converts file content to dictionary or opposite way. File needs to be in format of SupportedFormats
    """

    class SupportedFormat(Enum):
        json = ".json"
        json5 = ".json5"
        jsonc = ".jsonc"
        yaml = ".yaml"
        yml = ".yml"


    def __init__(self):
        pass

    def to_dict(self, file_name: str, content) -> dict:

        ext = Path(file_name).suffix
        if ext not in (item.value for item in self.SupportedFormat):
            raise ValueError(f"Unsupported file format: {ext}")

        if ext == ".json":
            return json.loads(content)
        if ext in [".json5", ".jsonc"]:
            return json5.loads(content)
        if ext in [".yaml", ".yml"]:
            return yaml.safe_load(content)

        return None
    
    def to_file_content(self, file_name: str, content: dict, **kwargs) -> str:

        ext = Path(file_name).suffix
        if ext not in (item.value for item in self.SupportedFormat):
            raise ValueError(f"Unsupported file format: {ext}")

        if ext == ".json":
            return json.dumps(content, **kwargs)
        if ext in [".json5", ".jsonc"]:
            return json5.dumps(content, **kwargs)
        if ext in [".yaml", ".yml"]:
            return yaml.dump(content, **kwargs)

        return None

