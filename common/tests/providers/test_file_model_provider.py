# import json
import os

import pytest
from poweretl.utils import FileEntry

from poweretl.common import FileModelProvider


def test_get_model():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    model_1 = FileModelProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/model", r"\.jsonc?$")],
        param_paths=[FileEntry(f"{module_dir}/_data/model", r"\.yaml$")],
        encoding="utf-8",
    )

    model_2 = FileModelProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/result", r"model\.json$")],
        encoding="utf-8",
    )

    model_1 = model_1.get_model()
    # json_str = json.dumps(asdict(model_1), indent=4, sort_keys=True)
    # with open(f'{module_dir}/_data/result/model.json', "w+", encoding="utf-8") as f:
    #     f.write(json_str)

    model_2 = model_2.get_model()
    assert model_1 == model_2, "Models are not equal"


def test_get_model_empty():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    model_1 = FileModelProvider(
        config_paths=[FileEntry(f"{module_dir}/_data/model", r"\.dummy.json$")],
        encoding="utf-8",
    )

    model_1 = model_1.get_model()

    assert model_1 == {}, "Model is not empty"


def test_get_model_unsupported():
    module_dir = os.path.dirname(os.path.abspath(__file__))



    with pytest.raises(ValueError, match="Unsupported file format: .txt"):
        model_1 = FileModelProvider(
            config_paths=[FileEntry(f"{module_dir}/_data/model", r"\.txt$")],
            encoding="utf-8",
        )        
        model_1.get_model()
