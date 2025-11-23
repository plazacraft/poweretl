# pylint: disable=W0621, W0613

import json

import json5
import pytest
import yaml

from poweretl.utils import FileSerializer  # Adjust import path as needed


@pytest.fixture
def serializer():
    return FileSerializer(indent=2, sort_keys=True)


@pytest.mark.parametrize(
    "ext,serialize,deserialize",
    [
        (".json", json.dumps, json.loads),
        (".json5", json5.dumps, json5.loads),
        (".jsonc", json5.dumps, json5.loads),
        (".yaml", yaml.dump, yaml.safe_load),
        (".yml", yaml.dump, yaml.safe_load),
    ],
)
def test_to_dict_supported_formats(ext, serialize, deserialize, serializer):
    data = {"name": "Roman", "active": True, "count": 3}
    content = serialize(data)
    result = serializer.to_dict(f"test{ext}", content)
    assert result == data


@pytest.mark.parametrize(
    "ext,serialize,deserialize",
    [
        (".json", json.dumps, json.loads),
        (".json5", json5.dumps, json5.loads),
        (".jsonc", json5.dumps, json5.loads),
        (".yaml", yaml.dump, yaml.safe_load),
        (".yml", yaml.dump, yaml.safe_load),
    ],
)
def test_to_file_content_supported_formats(ext, serialize, deserialize, serializer):
    data = {"name": "Roman", "active": True, "count": 3}
    content = serializer.to_file_content(f"test{ext}", data)
    parsed = deserialize(content)
    assert parsed == data


def test_to_dict_unsupported_format(serializer):
    with pytest.raises(ValueError, match="Unsupported file format: .txt"):
        serializer.to_dict("file.txt", "irrelevant")


def test_to_file_content_unsupported_format(serializer):
    with pytest.raises(ValueError, match="Unsupported file format: .ini"):
        serializer.to_file_content("config.ini", {"key": "value"})


def test_round_trip_json5(serializer):
    data = {"x": 1, "y": [1, 2, 3], "z": {"a": True}}
    content = serializer.to_file_content("data.json5", data)
    parsed = serializer.to_dict("data.json5", content)
    assert parsed == data
