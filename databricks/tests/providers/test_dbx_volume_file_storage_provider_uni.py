# pylint: disable=protected-access, W0621

from unittest.mock import Mock

import pytest

from poweretl.databricks.providers import DbxVolumeFileStorageProvider


@pytest.fixture
def mock_spark():
    """Mock SparkSession with necessary methods."""
    spark = Mock()
    spark.read = Mock()
    return spark


@pytest.fixture
def mock_dbutils():
    """Mock dbutils with filesystem operations."""
    dbutils = Mock()
    dbutils.fs = Mock()
    return dbutils


@pytest.fixture
def provider(mock_spark, mock_dbutils):
    """Create DbxVolumeFileStorageProvider instance with mocked dependencies."""
    return DbxVolumeFileStorageProvider(mock_spark, mock_dbutils)


class TestDbxVolumeFileStorageProvider:
    """Test cases for DbxVolumeFileStorageProvider."""

    def test_init(self, mock_spark, mock_dbutils):
        """Test provider initialization."""
        provider = DbxVolumeFileStorageProvider(mock_spark, mock_dbutils)

        assert provider._spark == mock_spark
        assert provider._dbutils == mock_dbutils

    def test_is_dir_method_with_callable_isdir(self, provider):
        """Test _is_dir method with callable isDir method."""
        mock_file = Mock()
        mock_file.isDir = Mock(return_value=True)

        result = provider._is_dir(mock_file)

        assert result is True
        mock_file.isDir.assert_called_once()

    def test_is_dir_method_with_empty_name(self, provider):
        """Test _is_dir method with empty name (directory)."""
        mock_file = Mock()
        mock_file.name = ""
        del mock_file.isDir  # Remove isDir to test fallback

        result = provider._is_dir(mock_file)

        assert result is True

    def test_is_dir_method_with_non_empty_name(self, provider):
        """Test _is_dir method with non-empty name (file)."""
        mock_file = Mock()
        mock_file.name = "file.txt"
        del mock_file.isDir  # Remove isDir to test fallback

        result = provider._is_dir(mock_file)

        assert result is False

    def test_get_first_file_or_folder_ascending(self, provider, mock_dbutils):
        """Test getting first file/folder in ascending order."""
        # Mock file list response
        mock_file1 = Mock()
        mock_file1.name = "file_b.txt"
        mock_file1.path = "/path/file_b.txt"
        # Remove isDir method so it falls back to name-based detection
        del mock_file1.isDir

        mock_file2 = Mock()
        mock_file2.name = "file_a.txt"
        mock_file2.path = "/path/file_a.txt"
        # Remove isDir method so it falls back to name-based detection
        del mock_file2.isDir

        mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2]

        result = provider.get_first_file_or_folder("/test/path", ascending=True)

        assert result == ("/path/file_a.txt", False)
        mock_dbutils.fs.ls.assert_called_once_with("/test/path")

    def test_get_first_file_or_folder_descending(self, provider, mock_dbutils):
        """Test getting first file/folder in descending order."""
        mock_file1 = Mock()
        mock_file1.name = ""  # Empty name = directory
        mock_file1.path = "/path"
        del mock_file1.isDir

        mock_file2 = Mock()
        mock_file2.name = "file_b.txt"
        mock_file2.path = "/path/file_b.txt"
        del mock_file2.isDir

        mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2]

        result = provider.get_first_file_or_folder("/test/path", ascending=False)

        assert result == ("/path/file_b.txt", False)

    def test_get_first_file_or_folder_empty_directory(self, provider, mock_dbutils):
        """Test getting first file/folder from empty directory."""
        mock_dbutils.fs.ls.return_value = []

        result = provider.get_first_file_or_folder("/test/path")

        assert result is None

    def test_get_folders_list_non_recursive(self, provider, mock_dbutils):
        """Test getting immediate subdirectories only."""
        mock_dir = Mock()
        mock_dir.path = "/path/subdir"
        mock_dir.name = ""  # Empty name = directory
        del mock_dir.isDir

        mock_file = Mock()
        mock_file.path = "/path/file.txt"
        mock_file.name = "file.txt"  # Non-empty name = file
        del mock_file.isDir

        mock_dbutils.fs.ls.return_value = [mock_dir, mock_file]

        result = provider.get_folders_list("/test/path", recursive=False)

        assert result == ["/path/subdir"]
        mock_dbutils.fs.ls.assert_called_once_with("/test/path")

    def test_get_folders_list_recursive(self, provider, mock_dbutils):
        """Test getting folders recursively."""
        # Mock initial directory listing
        mock_dir1 = Mock()
        mock_dir1.path = "/path/dir1"
        mock_dir1.name = ""  # Empty name = directory
        del mock_dir1.isDir

        mock_file1 = Mock()
        mock_file1.path = "/path/file1.txt"
        mock_file1.name = "file1.txt"  # Non-empty name = file
        del mock_file1.isDir

        # Mock subdirectory listing
        mock_dir2 = Mock()
        mock_dir2.path = "/path/dir1/dir2"
        mock_dir2.name = ""  # Empty name = directory
        del mock_dir2.isDir

        mock_file2 = Mock()
        mock_file2.path = "/path/dir1/file2.txt"
        mock_file2.name = "file2.txt"  # Non-empty name = file
        del mock_file2.isDir

        def ls_side_effect(path):
            if path == "/test/path":
                return [mock_dir1, mock_file1]
            if path == "/path/dir1":
                return [mock_dir2, mock_file2]

            return []

        mock_dbutils.fs.ls.side_effect = ls_side_effect

        result = provider.get_folders_list("/test/path", recursive=True)

        assert "/path/dir1" in result
        assert "/path/dir1/dir2" in result
        assert len(result) == 2

    def test_get_files_list_non_recursive(self, provider, mock_dbutils):
        """Test getting immediate files only."""
        mock_dir = Mock()
        mock_dir.path = "/path/subdir"
        mock_dir.name = ""  # Empty name = directory
        del mock_dir.isDir

        mock_file = Mock()
        mock_file.path = "/path/file.txt"
        mock_file.name = "file.txt"  # Non-empty name = file
        del mock_file.isDir

        mock_dbutils.fs.ls.return_value = [mock_dir, mock_file]

        result = provider.get_files_list("/test/path", recursive=False)

        assert result == ["/path/file.txt"]

    def test_get_files_list_recursive(self, provider, mock_dbutils):
        """Test getting files recursively."""
        # Mock initial directory listing
        mock_dir1 = Mock()
        mock_dir1.path = "/path/dir1"
        mock_dir1.name = ""  # Empty name = directory
        del mock_dir1.isDir

        mock_file1 = Mock()
        mock_file1.path = "/path/file1.txt"
        mock_file1.name = "file1.txt"  # Non-empty name = file
        del mock_file1.isDir

        # Mock subdirectory listing
        mock_file2 = Mock()
        mock_file2.path = "/path/dir1/file2.txt"
        mock_file2.name = "file2.txt"  # Non-empty name = file
        del mock_file2.isDir

        def ls_side_effect(path):
            if path == "/test/path":
                return [mock_dir1, mock_file1]
            if path == "/path/dir1":
                return [mock_file2]
            return []

        mock_dbutils.fs.ls.side_effect = ls_side_effect

        result = provider.get_files_list("/test/path", recursive=True)

        assert "/path/file1.txt" in result
        assert "/path/dir1/file2.txt" in result
        assert len(result) == 2

    def test_get_file_str_content_success(self, provider, mock_spark, mock_dbutils):
        """Test successful file content reading."""
        # Mock file existence check
        mock_dbutils.fs.ls.return_value = [Mock()]

        # Mock Spark DataFrame
        mock_row = {"value": "file content here"}
        mock_df = Mock()
        mock_df.collect.return_value = [mock_row]

        mock_read = Mock()
        mock_read.option.return_value = mock_read
        mock_read.text.return_value = mock_df
        mock_spark.read = mock_read

        result = provider.get_file_str_content("/test/file.txt")

        assert result == "file content here"
        mock_dbutils.fs.ls.assert_called_once_with("/test/file.txt")
        mock_read.option.assert_called_once_with("wholetext", "true")
        mock_read.text.assert_called_once_with("/test/file.txt")

    def test_get_file_str_content_file_not_exists(self, provider, mock_dbutils):
        """Test file content reading when file doesn't exist."""
        # Mock file existence check to raise exception
        mock_dbutils.fs.ls.side_effect = Exception("File not found")

        result = provider.get_file_str_content("/test/nonexistent.txt")

        assert result is None
        mock_dbutils.fs.ls.assert_called_once_with("/test/nonexistent.txt")

    def test_get_file_str_content_spark_read_error(
        self, provider, mock_spark, mock_dbutils
    ):
        """Test file content reading when Spark read fails."""
        # Mock file existence check passes
        mock_dbutils.fs.ls.return_value = [Mock()]

        # Mock Spark read to raise exception
        mock_read = Mock()
        mock_read.option.return_value = mock_read
        mock_read.text.side_effect = Exception("Spark read error")
        mock_spark.read = mock_read

        # This should raise the exception since it's not caught
        with pytest.raises(Exception, match="Spark read error"):
            provider.get_file_str_content("/test/file.txt")

    def test_upload_file_str_success(self, provider, mock_dbutils):
        """Test successful file upload."""
        content = "test content"
        file_path = "/test/output.txt"

        provider.upload_file_str(file_path, content)

        mock_dbutils.fs.put.assert_called_once_with(file_path, content, overwrite=True)

    def test_upload_file_str_dbutils_error(self, provider, mock_dbutils):
        """Test file upload when dbutils.fs.put fails."""
        mock_dbutils.fs.put.side_effect = Exception("Upload failed")

        # This should raise the exception since it's not caught
        with pytest.raises(Exception, match="Upload failed"):
            provider.upload_file_str("/test/output.txt", "content")

    def test_get_file_str_content_empty_file(self, provider, mock_spark, mock_dbutils):
        """Test reading empty file content."""
        # Mock file existence check
        mock_dbutils.fs.ls.return_value = [Mock()]

        # Mock Spark DataFrame with empty content
        mock_row = {"value": ""}
        mock_df = Mock()
        mock_df.collect.return_value = [mock_row]

        mock_read = Mock()
        mock_read.option.return_value = mock_read
        mock_read.text.return_value = mock_df
        mock_spark.read = mock_read

        result = provider.get_file_str_content("/test/empty.txt")

        assert result == ""

    def test_get_first_file_or_folder_case_insensitive_sorting(
        self, provider, mock_dbutils
    ):
        """Test that file sorting is case insensitive."""
        mock_file1 = Mock()
        mock_file1.name = "File_B.txt"
        mock_file1.path = "/path/File_B.txt"
        del mock_file1.isDir

        mock_file2 = Mock()
        mock_file2.name = "file_a.txt"
        mock_file2.path = "/path/file_a.txt"
        del mock_file2.isDir

        mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2]

        result = provider.get_first_file_or_folder("/test/path", ascending=True)

        # Should return file_a.txt as it comes first alphabetically (case insensitive)
        assert result == ("/path/file_a.txt", False)
