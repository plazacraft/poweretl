from pathlib import Path
from poweretl.common.utils.file.FileCommandSplitter import *
import poweretl.common.utils.tests as common_tests
import pytest
import os


def test_parse_sample_file(tmp_path):
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"
    module_dir = os.path.dirname(os.path.abspath(__file__))


    sample = data_dir / "01.sample_meta.sql"

    provider = FileCommandSplitter()
    results = provider.read_files([sample])

    tests_config = common_tests.ConfigReader(
        file_path=f'{module_dir}/_config.json'
    )
    config_test_name = "FileCommandSplitter.test_parse_sample_file"


    assert len(results) == 4
    assert isinstance(results[0], CommandEntry)
    assert results[0].version == tests_config.get_expectations(config_test_name)[0]
    assert tests_config.get_expectations(config_test_name)[1] in results[0].command
    assert tests_config.get_expectations(config_test_name)[2] in results[1].command
    assert results[2].version == tests_config.get_expectations(config_test_name)[3]
    assert tests_config.get_expectations(config_test_name)[4] in results[2].command
    assert tests_config.get_expectations(config_test_name)[5] in results[3].command


def test_read_files_multiple(tmp_path):
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"
    module_dir = os.path.dirname(os.path.abspath(__file__))
    f1 = data_dir / "01.sample_meta.sql"
    f2 = data_dir / "02.sample_meta_2.sql"

    tests_config = common_tests.ConfigReader(
        file_path=f'{module_dir}/_config.json'
    )

    config_test_name = "FileCommandSplitter.test_read_files_multiple"

    provider = FileCommandSplitter()
    results = provider.read_files([f1, f2])

    assert len(results) == 7
    assert results[4].version == '1.1'
    assert results[6].version == '2.1'


def test_semver_sorting(tmp_path):
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"
    module_dir = os.path.dirname(os.path.abspath(__file__))
    v1_10 = data_dir / "03.sample_semver_1.sql"
    v1_2 = data_dir / "04.sample_semver_2.sql"

    tests_config = common_tests.ConfigReader(
        file_path=f'{module_dir}/_config.json'
    )
    config_test_name = "FileCommandSplitter.test_semver_sorting"

    provider = FileCommandSplitter()
    # provide files in reverse order to ensure sort happens
    results = provider.read_files([v1_10, v1_2])

    # Semantic sort should put 1.2 before 1.10
    versions = [r.version for r in results if r.command.strip()]
    assert '1.2' in versions
    assert '1.10' in versions
    assert versions.index('1.2') < versions.index('1.10')
