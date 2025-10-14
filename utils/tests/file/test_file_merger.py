import os

import pytest

from poweretl.utils.file import FileEntry, FileMerger, MultiFileReader
from poweretl.utils.text import TokensReplacer


def test_merge_files():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data/FileMerger"
    tokens_replacer = TokensReplacer(
        re_start=r"(/\*<)|(<)", re_end=r"(>\*/)|(>)", re_escape=r"\^"
    )

    merger = FileMerger()
    config_reader_1 = MultiFileReader([FileEntry(f"{data_dir}/config", r"\.jsonc?$")])
    config_files_1 = config_reader_1.get_files_with_content()

    param_reader_1 = MultiFileReader([FileEntry(f"{data_dir}/config", r"\.yaml$")])
    param_files_1 = param_reader_1.get_files_with_content()

    params_1 = merger.merge(param_files_1)
    config_contents_1 = [
        (config, tokens_replacer.replace(tokens=params_1, text=content))
        for config, content in config_files_1
    ]

    config_1 = merger.merge(config_contents_1)

    config_reader_2 = MultiFileReader([FileEntry(f"{data_dir}/result", r"\.json$")])
    config_files_2 = config_reader_2.get_files_with_content()
    config_2 = merger.merge(config_files_2)

    # json_str=config_1.to_json(config_2, dump_params={"indent": 4, "sort_keys": True})
    # with open(f'{module_dir}/_data/result/merged.json', "w+", encoding="utf-8") as f:
    #     f.write(json_str)

    assert config_1 == config_2, "Result are not equal"


def test_merge_files_empty():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data/FileMerger"

    merger = FileMerger()
    config_reader_1 = MultiFileReader(
        [FileEntry(f"{data_dir}/config", r"\.dummy\.json$")]
    )
    config_files_1 = config_reader_1.get_files_with_content()

    assert merger.merge(config_files_1) is None, "Result is not empty"


def test_merge_files_unsupported():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data/FileMerger"

    merger = FileMerger()
    config_reader_1 = MultiFileReader([FileEntry(f"{data_dir}/config", r"\.txt$")])
    config_files_1 = config_reader_1.get_files_with_content()

    with pytest.raises(ValueError, match="Unsupported file extension: .txt"):
        merger.merge(config_files_1)
