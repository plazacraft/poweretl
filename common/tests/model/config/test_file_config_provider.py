import os

import pytest

from poweretl.common.model.config import FileConfigProvider
from poweretl.utils.file import FileEntry


def test_get_model():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    config_1 = FileConfigProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/config", r"\.jsonc?$")],
        param_paths=[FileEntry(f"{module_dir}/_data/config", r"\.yaml$")],
        encoding="utf-8",
    )

    config_2 = FileConfigProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/result", r"\.json$")],
        encoding="utf-8",
    )

    model_1 = config_1.get_model()
    # json_str = config_1.to_json(model_1, dump_params={"indent": 4, "sort_keys": True})
    # with open(f'{module_dir}/_data/result/merged.json', "w+", encoding="utf-8") as f:
    #     f.write(json_str)

    model_2 = config_2.get_model()
    assert model_1 == model_2, "Models are not equal"

    json_str = config_1.to_json(model_1, dump_params={"indent": 4, "sort_keys": True})
    with open(f"{module_dir}/_data/result/merged.json", "r", encoding="utf-8") as f:
        file_str = f.read()
    assert json_str == file_str, "JSON strings are not equal"


def test_get_model_empty():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    config_1 = FileConfigProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/config", r"\.dummy.json$")],
        encoding="utf-8",
    )

    model_1 = config_1.get_model()

    assert model_1 == {}, "Model is not empty"


def test_get_model_unsupported():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    config_1 = FileConfigProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/config", r"\.txt$")],
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file extension: .txt"):
        config_1.get_model()
