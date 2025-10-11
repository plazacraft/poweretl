from poweretl.utils.file import *
import poweretl.utils.tests as common_tests
import pytest
import os
from pathlib import Path


def test_get_files():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    data_dir = f'{module_dir}/_data/MultiFileReader'
    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r'global\.json$', False),
            FileEntry(f"{data_dir}/01", r'\.config\.json$'),
            FileEntry(f"{data_dir}/02", r'\.config\.json$')
        ],
        encoding='utf-8'
    )

    tests_config = common_tests.ConfigReader(
        file_path=f'{module_dir}/_config.json'
    )

    expected_files = tests_config.get_expectations('MultiFileReader.get_files')
    expected_files = [Path(eval(f'f"{f}"')) for f in expected_files]

    files = reader.get_files()
    assert files == expected_files

    files, contents = zip(*reader.get_files_with_content())

    assert list(files) == expected_files, "Files not loaded correctly"








    

