import json
from enum import Enum
from pathlib import Path

import json5
import yaml


class FileSerializer:
    """Converts file content to dictionary or opposite way.
    File needs to be in format of SupportedFormats"""

    class SupportedFormat(Enum):
        JSON = ".json"
        JSON5 = ".json5"
        JSONC = ".jsonc"
        YAML = ".yaml"
        YML = ".yml"

    def __init__(self, **serialization_params):
        self._serialization_params = serialization_params

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

    def to_file_content(self, file_name: str, content: dict) -> str:

        ext = Path(file_name).suffix
        if ext not in (item.value for item in self.SupportedFormat):
            raise ValueError(f"Unsupported file format: {ext}")

        if ext == ".json":
            return json.dumps(content, **self._serialization_params)
        if ext in [".json5", ".jsonc"]:
            return json5.dumps(content, **self._serialization_params)
        if ext in [".yaml", ".yml"]:
            return yaml.dump(content, **self._serialization_params)

        return None
