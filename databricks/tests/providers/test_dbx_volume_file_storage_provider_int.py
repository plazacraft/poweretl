# pylint: disable=protected-access

import pytest
from poweretl.databricks.providers import DbxVolumeFileStorageProvider
from poweretl.databricks.helpers import get_or_connect, Session
from poweretl.databricks.helpers.session_utils import *

def test_dbx_volume_file_storage_provider_int():
    global dbutils

    session = get_or_connect(dbutils=dbutils)
    spark = session.spark
    dbutils = session.dbutils


    storage_provider = DbxVolumeFileStorageProvider(spark, dbutils)

    test_path = "/Volumes/workspace/poweretl_tests/unit_tests/storage_provider"
    test_file_1 = f"{test_path}/file1.txt"
    test_file_2 = f"{test_path}/file2.txt"
    test_dir_1 = f"{test_path}/dir1"
    test_dir_2 = f"{test_path}/dir2"

    # Setup test files and directories
    dbutils.fs.mkdirs(test_dir_1)
    dbutils.fs.mkdirs(test_dir_2)
    #dbutils.fs.put(test_file_1, "Content of file 1", True)
    #dbutils.fs.put(test_file_2, "Content of file 2", True)
    storage_provider.upload_file_str(test_file_1, "Content of file 1")
    storage_provider.upload_file_str(test_file_2, "Content of file 2")

    # Test get_first_file_or_folder
    first_file, is_dir = storage_provider.get_first_file_or_folder(test_path, ascending=True)
    first_file = first_file.rstrip('/')
    assert first_file in [test_file_1, test_file_2, test_dir_1, test_dir_2]
    assert is_dir == (first_file in [test_dir_1, test_dir_2])

    # Test get_folders_list
    folders = storage_provider.get_folders_list(test_path, recursive=False)
    folders = [folder.rstrip('/') for folder in folders]
    assert set(folders) == {test_dir_1, test_dir_2}

    # Test get_files_list
    files = storage_provider.get_files_list(test_path, recursive=False)
    assert set(files) == {test_file_1, test_file_2}

    # Cleanup
    dbutils.fs.rm(test_path, True)