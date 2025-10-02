from poweretl.common.model.config._MultiFileReader import _MultiFileReader
import poweretl.common.tests as common_tests
import pytest
import os
from pathlib import Path


def test_get_files():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    reader = _MultiFileReader(
        regex=r'\.config\.json$',
        file_paths=[
            f'{module_dir}/_data/01',
            f'{module_dir}/_data/02'
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
    

