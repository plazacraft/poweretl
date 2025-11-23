# pylint: disable=W0621, W0613
# Mostly AI

import tempfile
from pathlib import Path

import pytest

from poweretl.utils import OSFileStorageProvider  # Adjust import as needed


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


@pytest.fixture
def provider():
    return OSFileStorageProvider()


def test_upload_and_read_file(provider, temp_dir):
    file_path = temp_dir / "test.txt"
    content = "Hello, world!"
    provider.upload_file_str(str(file_path), content)

    assert file_path.exists()
    assert file_path.read_text(encoding="utf-8") == content

    read_content = provider.get_file_str_content(str(file_path))
    assert read_content == content


def test_get_files_list_non_recursive(provider, temp_dir):
    (temp_dir / "file1.txt").write_text("a")
    (temp_dir / "file2.txt").write_text("b")
    (temp_dir / "subdir").mkdir()

    files = provider.get_files_list(str(temp_dir), recursive=False)
    assert len(files) == 2
    assert all(Path(f).name in ["file1.txt", "file2.txt"] for f in files)


def test_get_files_list_recursive(provider, temp_dir):
    (temp_dir / "file1.txt").write_text("a")
    subdir = temp_dir / "subdir"
    subdir.mkdir()
    (subdir / "file2.txt").write_text("b")

    files = provider.get_files_list(str(temp_dir), recursive=True)
    assert len(files) == 2
    assert any("file2.txt" in str(f) for f in files)


def test_get_folders_list_non_recursive(provider, temp_dir):
    (temp_dir / "sub1").mkdir()
    (temp_dir / "sub2").mkdir()
    (temp_dir / "file.txt").write_text("data")

    folders = provider.get_folders_list(str(temp_dir), recursive=False)
    assert len(folders) == 2
    assert all(Path(f).name in ["sub1", "sub2"] for f in folders)


def test_get_folders_list_recursive(provider, temp_dir):
    (temp_dir / "sub1").mkdir()
    nested = temp_dir / "sub1" / "nested"
    nested.mkdir()

    folders = provider.get_folders_list(str(temp_dir), recursive=True)
    assert len(folders) == 2
    assert any("nested" in str(f) for f in folders)


def test_get_first_file_or_folder_ascending(provider, temp_dir):
    (temp_dir / "b.txt").write_text("b")
    (temp_dir / "a.txt").write_text("a")

    first, is_dir = provider.get_first_file_or_folder(str(temp_dir), ascending=True)
    assert Path(first).name == "a.txt"
    assert is_dir is False


def test_get_first_file_or_folder_descending(provider, temp_dir):
    (temp_dir / "b.txt").write_text("b")
    (temp_dir / "a.txt").write_text("a")

    first, is_dir = provider.get_first_file_or_folder(str(temp_dir), ascending=False)
    assert Path(first).name == "b.txt"
    assert is_dir is False
