import pytest

from pathlib import Path

from poweretl.utils.providers.mem_file_storage_provider import MemFileStorageProvider


def test_upload_and_get_file_content():
    provider = MemFileStorageProvider()

    provider.upload_file_str("/tmp/a/file1.txt", "hello")
    provider.upload_file_str("/tmp/a/file2.txt", "world")

    assert provider.get_file_str_content("/tmp/a/file1.txt") == "hello"
    assert provider.get_file_str_content("/tmp/a/file2.txt") == "world"
    # missing file returns None
    assert provider.get_file_str_content("/tmp/a/missing.txt") is None


def test_get_first_file_or_folder_empty():
    provider = MemFileStorageProvider()
    assert provider.get_first_file_or_folder("/tmp") is None


def test_get_first_file_or_folder_prefers_dirs_and_respects_order():
    provider = MemFileStorageProvider()

    # create files under /tmp: a file in dir1, a top-level file, and dir2
    provider.upload_file_str("/tmp/dirB/file1.json", "x")
    provider.upload_file_str("/tmp/dirA/file2.json", "y")
    provider.upload_file_str("/tmp/file_top.txt", "z")

    # ascending=True -> by name, case-insensitive -> dirA comes first
    chosen, is_dir = provider.get_first_file_or_folder("/tmp", ascending=True)
    assert Path(chosen).as_posix() == Path("/tmp/dirA").as_posix()
    assert is_dir is True

    # descending should pick dirB
    chosen2, is_dir2 = provider.get_first_file_or_folder("/tmp", ascending=False)
    assert Path(chosen2).as_posix() == Path("/tmp/dirB").as_posix()
    assert is_dir2 is True


def test_get_first_file_or_folder_returns_file_when_no_dirs():
    provider = MemFileStorageProvider()

    provider.upload_file_str("/tmp/file_a.txt", "a")
    provider.upload_file_str("/tmp/file_b.txt", "b")

    chosen, is_dir = provider.get_first_file_or_folder("/tmp", ascending=True)
    assert Path(chosen).as_posix() == Path("/tmp/file_a.txt").as_posix()
    assert is_dir is False


def test_get_folders_list_empty():
    provider = MemFileStorageProvider()
    assert provider.get_folders_list("/noexist") == []


def test_get_folders_list_non_recursive_and_recursive():
    provider = MemFileStorageProvider()

    provider.upload_file_str("/base/a/file1.txt", "1")
    provider.upload_file_str("/base/a/sub/x.txt", "x")
    provider.upload_file_str("/base/b/file2.txt", "2")

    nonrec = provider.get_folders_list("/base", recursive=False)
    # should contain immediate child folders
    assert "/base/a" in nonrec or Path("/base/a").as_posix() in nonrec
    assert "/base/b" in nonrec or Path("/base/b").as_posix() in nonrec

    rec = provider.get_folders_list("/base", recursive=True)
    # should contain nested folder '/base/a/sub'
    assert any(p.endswith("/base/a/sub") or p.endswith("base/a/sub") for p in rec)
