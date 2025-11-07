# pylint: disable=R0801

from pathlib import Path
from poweretl.utils import IFileStorageWriter
from pyspark.sql import SparkSession  # pylint: disable=C0411

from databricks.sdk.dbutils import RemoteDbUtils


class DbxVolumeFileStorageProvider(IFileStorageWriter):

    def __init__(self, spark: SparkSession, dbutils):
        self._spark = spark
        self._dbutils: RemoteDbUtils = dbutils

    def _is_dir(self, file_info) -> bool:
        if hasattr(file_info, "isDir") and callable(getattr(file_info, "isDir")):
            return file_info.isDir()

        if not file_info.name:
            return True

        return False

    def get_first_file_or_folder(
        self, path: str, ascending: bool = True
    ) -> tuple[str, bool]:
        files = self._dbutils.fs.ls(path)
        if files:
            files = sorted(files, key=lambda f: f.name.lower(), reverse=not ascending)
            first_file = files[0]
            return (first_file.path, self._is_dir(first_file))
        return None

    def get_folders_list(self, path: str, recursive: bool = False) -> list[str]:
        if recursive:
            # Get all files/folders recursively, then filter for directories
            all_items = []

            def _collect_recursive(current_path):
                items = self._dbutils.fs.ls(current_path)
                for item in items:
                    if self._is_dir(item):
                        all_items.append(item.path)
                        _collect_recursive(item.path)

            _collect_recursive(path)
            return all_items

        # Get only immediate subdirectories
        items = self._dbutils.fs.ls(path)
        return [item.path for item in items if self._is_dir(item)]

    def get_files_list(self, path: str, recursive: bool = False) -> list[str]:
        if recursive:
            # Get all files recursively
            all_files = []

            def _collect_recursive(current_path):
                items = self._dbutils.fs.ls(current_path)
                for item in items:
                    if self._is_dir(item):
                        _collect_recursive(item.path)
                    else:
                        all_files.append(item.path)

            _collect_recursive(path)
            return all_files

        # Get only immediate files
        items = self._dbutils.fs.ls(path)
        return [item.path for item in items if not self._is_dir(item)]

    def get_file_str_content(self, full_path: str) -> str:
        path = str(Path(full_path).as_posix())
        try:
            # Check if file exists first
            self._dbutils.fs.ls(path)
        except Exception:  # pylint: disable=W0718
            # File doesn't exist or path is invalid
            return None

        # df = self._spark.read.text(full_path)
        # text_str = "\n".join(row["value"] for row in df.collect())

        df = self._spark.read.option("wholetext", "true").text(path)
        text_str = df.collect()[0]["value"]

        return text_str

    def upload_file_str(self, full_path: str, content: str):
        # Use dbutils.fs.put to write content directly to the volume
        self._dbutils.fs.put(str(Path(full_path).as_posix()), content, overwrite=True)
