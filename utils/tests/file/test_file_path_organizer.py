from datetime import datetime

import pytest

from poweretl.utils import FilePathOrganizer


@pytest.fixture
def timestamp():
    return datetime(2025, 10, 15, 10, 1, 0)


@pytest.mark.parametrize(
    "path_pattern,file_pattern,expected_folder,expected_file",
    [
        ("%Y/%m-%d", "%H-%M-%S", "2025/10-15", "10-01-00"),
        ("%Y-%m-%d", "%H%M", "2025-10-15", "1001"),
        ("%Y/%A", "%H-%M", "2025/Wednesday", "10-01"),
        ("%Y/%m/%d", "%H-%M-%S", "2025/10/15", "10-01-00"),
    ],
)
def test_get_name_valid(
    timestamp,  # pylint: disable=W0621
    path_pattern,
    file_pattern,
    expected_folder,
    expected_file,
):
    organizer = FilePathOrganizer(path_pattern, file_pattern)
    folder, file_prefix = organizer.get_name(timestamp)
    assert folder == expected_folder
    assert file_prefix == expected_file


@pytest.mark.parametrize(
    "pattern,expected",
    [
        ("%Y/%m:%d", "2025/1015"),
        ("%Y\\%m|%d", "2025\\1015"),
        ("%Y<%m>%d", "20251015"),
        ("%Y*%m?%d", "20251015"),
    ],
)
def test_sanitization(timestamp, pattern, expected):  # pylint: disable=W0621
    organizer = FilePathOrganizer(path_pattern=pattern, file_pattern=pattern)
    folder, file_prefix = organizer.get_name(timestamp)
    assert folder == expected
    assert file_prefix == expected
