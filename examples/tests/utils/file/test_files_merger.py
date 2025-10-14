import json
import os

from poweretl.utils.file import FileEntry, FileMerger, MultiFileReader
from poweretl.utils.text import TokensReplacer


def test_merge_files():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data/files_merger"

    tokens_replacer = TokensReplacer(
        re_start=r"(/\*<)|(<)", re_end=r"(>\*/)|(>)", re_escape=r"\^"
    )

    merger = FileMerger()
    config_reader = MultiFileReader([FileEntry(f"{data_dir}", r"\.jsonc?$")])
    config_files = config_reader.get_files_with_content()

    param_reader = MultiFileReader([FileEntry(f"{data_dir}", r"\.yaml?$")])
    param_files = param_reader.get_files_with_content()

    params = merger.merge(param_files)
    config_contents = [
        (config, tokens_replacer.replace(tokens=params, text=content))
        for config, content in config_files
    ]

    config = merger.merge(config_contents)

    json_str = json.dumps(config, indent=4, sort_keys=True)
    with open(f"{module_dir}/config.json", "w+", encoding="utf-8") as f:
        f.write(json_str)
