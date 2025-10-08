from poweretl.common.utils.file import *
import poweretl.common.utils.tests as common_tests
import pytest
import os
from pathlib import Path


def test_get_files():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    data_dir = f'{module_dir}/_data'
    reader = MultiFileReader(
        file_paths=[
            MultiFileReader.FileEntry(data_dir, r'global\.json$', False),
            MultiFileReader.FileEntry(f"{data_dir}/01", r'\.config\.json$'),
            MultiFileReader.FileEntry(f"{data_dir}/02", r'\.config\.json$')
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

    files, content = reader.get_files_with_content()
    assert files == expected_files


    

