import os
from pathlib import Path

import poweretl.utils.tests as common_tests
from poweretl.utils import CommandEntry, FileCommandSplitter, FileEntry, MultiFileReader


def test_parse_sample_file():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"
    module_dir = os.path.dirname(os.path.abspath(__file__))

    reader = MultiFileReader(
        file_paths=[FileEntry(data_dir, r"01\.sample_meta\.sql", False)],
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

    # Verify steps start at 1 and increment per version
    steps_by_version = {}
    for r in results:
        steps_by_version.setdefault(r.version, []).append(r.step)
    for ver, steps in steps_by_version.items():
        assert steps == list(
            range(1, len(steps) + 1)
        ), f"Steps for version {ver} should start at 1 and increment"


def test_read_files_multiple():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"

    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r"01\.sample_meta\.sql", False),
            FileEntry(data_dir, r"02\.sample_meta_2\.sql", False),
        ],
    )

    provider = FileCommandSplitter()
    results = provider.get_commands(reader.get_files_with_content())

    assert len(results) == 7
    assert results[4].version == "1.1"
    assert results[6].version == "2.1"

    # Verify steps start at 1 and increment per version
    steps_by_version = {}
    for r in results:
        steps_by_version.setdefault(r.version, []).append(r.step)
    for ver, steps in steps_by_version.items():
        assert steps == list(
            range(1, len(steps) + 1)
        ), f"Steps for version {ver} should start at 1 and increment"


def test_semver_sorting():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"

    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r"03\.sample_semver_1\.sql", False),
            FileEntry(data_dir, r"04\.sample_semver_2\.sql", False),
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

    # Verify steps start at 1 and increment per version
    steps_by_version = {}
    for r in results:
        steps_by_version.setdefault(r.version, []).append(r.step)
    for ver, steps in steps_by_version.items():
        assert steps == list(
            range(1, len(steps) + 1)
        ), f"Steps for version {ver} should start at 1 and increment"


def test_filter_by_version_and_step():
    data_dir = Path(__file__).parent / "_data/FileCommandSplitter"

    reader = MultiFileReader(
        file_paths=[
            FileEntry(data_dir, r"01\.sample_meta\.sql", False),
            FileEntry(data_dir, r"02\.sample_meta_2\.sql", False),
        ],
    )

    provider = FileCommandSplitter()
    all_results = provider.get_commands(reader.get_files_with_content())
    assert all_results, "Expected to parse at least one command"

    # Pick a pivot in the middle to filter after
    pivot = all_results[len(all_results) // 2]
    filtered = provider.get_commands(
        reader.get_files_with_content(), version=pivot.version, step=pivot.step
    )

    # Build expected using the same ordering rule
    def greater_than(e, v, s):
        if provider.version_key(e.version or "") > provider.version_key(v or ""):
            return True
        if provider.version_key(e.version or "") == provider.version_key(
            v or ""
        ) and e.step > (s or 0):
            return True
        return False

    expected = [e for e in all_results if greater_than(e, pivot.version, pivot.step)]

    assert [(e.version, e.step, e.command.strip()) for e in filtered] == [
        (e.version, e.step, e.command.strip()) for e in expected
    ], "Filtered results should match entries strictly greater than (version, step)"

    # None markers should not filter anything
    no_filter = provider.get_commands(
        reader.get_files_with_content(), version=None, step=None
    )
    assert [(e.version, e.step, e.command.strip()) for e in no_filter] == [
        (e.version, e.step, e.command.strip()) for e in all_results
    ], "None filters should return all entries"
