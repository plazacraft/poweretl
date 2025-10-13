import json
from abc import abstractmethod
from pathlib import Path

import json5
import yaml
from deepmerge import Merger, always_merger


class FileMerger:
    SUPPORTED_EXTENSIONS = [".json", ".yaml", ".yml", ".json5", ".jsonc"]

    """Merges files and returns dictionary object.
    Supported files are defined by SUPPORTED_EXTENSIONS

    Attributes:
        merger (Merger): Merger strategy,
            as default always_merger is used
    """

    def __init__(self, merger: Merger = always_merger):
        self._merger = merger
        # default strategy of always_merger
        # self._merger = Merger(
        #     [
        #         (dict, "merge"),
        #         (list, "append"),
        #         (set, "union")
        #     ],
        #     ["override"],
        #     ["override"]
        # )

    @abstractmethod
    def _to_dict(self, file: Path, content) -> dict:
        if not file.is_file():
            return None

        ext = file.suffix
        if ext not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file extension: {ext}")

        if ext == ".json":
            return json.loads(content)
        if ext in [".json5", ".jsonc"]:
            return json5.loads(content)
        if ext in [".yaml", ".yml"]:
            return yaml.safe_load(content)

        return None

    def merge(self, files: list[tuple[Path, str]]) -> dict:
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
