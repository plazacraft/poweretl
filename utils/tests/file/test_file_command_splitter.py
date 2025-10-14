import os
from pathlib import Path

import poweretl.utils.tests as common_tests
from poweretl.utils.file import CommandEntry, FileCommandSplitter, MultiFileReader, FileEntry


def test_parse_sample_file():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"
    module_dir = os.path.dirname(os.path.abspath(__file__))


    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r"01\.sample_meta\.sql", False)
        ],
    )


    provider = FileCommandSplitter()
    results = provider.get_commands(reader.get_files_with_content())

    tests_config = common_tests.ConfigReader(file_path=f"{module_dir}/_config.json")
    config_test_name = "FileCommandSplitter.test_parse_sample_file"

    assert len(results) == 4
    assert isinstance(results[0], CommandEntry)
    assert results[0].version == tests_config.get_expectations(config_test_name)[0]
    assert tests_config.get_expectations(config_test_name)[1] in results[0].command
    assert tests_config.get_expectations(config_test_name)[2] in results[1].command
    assert results[2].version == tests_config.get_expectations(config_test_name)[3]
    assert tests_config.get_expectations(config_test_name)[4] in results[2].command
    assert tests_config.get_expectations(config_test_name)[5] in results[3].command


def test_read_files_multiple():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"

    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r"01\.sample_meta\.sql", False),
            FileEntry(data_dir, r"02\.sample_meta_2\.sql", False)
        ],
    )


    provider = FileCommandSplitter()
    results = provider.get_commands(reader.get_files_with_content())

    assert len(results) == 7
    assert results[4].version == "1.1"
    assert results[6].version == "2.1"


def test_semver_sorting():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"


    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r"03\.sample_semver_1\.sql", False),
            FileEntry(data_dir, r"04\.sample_semver_2\.sql", False)
        ],
    )


    provider = FileCommandSplitter()
    # provide files in reverse order to ensure sort happens
    results = provider.get_commands(reader.get_files_with_content())

    # Semantic sort should put 1.2 before 1.10
    versions = [r.version for r in results if r.command.strip()]
    assert "1.2" in versions
    assert "1.10" in versions
    assert versions.index("1.2") < versions.index("1.10")
