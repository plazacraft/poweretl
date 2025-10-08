import json
from pathlib import Path
from typing import Dict, Any

class ConfigReader:
    def __init__(self, file_path: str):
        file_path = Path(file_path)

        self._data = {}
        json_data = None

        with open(file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        if (json_data):
            for item in json_data["tests"]:
                self._data[item["name"]] = item["expected"]
        
    def get_expectations(self, test_name) -> Any:
        return self._data.get(test_name, None)