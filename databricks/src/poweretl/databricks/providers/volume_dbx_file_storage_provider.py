# pylint: disable=R0801

from pyspark.sql import SparkSession
from databricks.sdk.dbutils import RemoteDbUtils

from poweretl.utils import IFileStorageWriter


class VolumeDbxFileStorageProvider(IFileStorageWriter):

    def __init__(self, spark: SparkSession, dbutils):
        self._spark = spark
        self._dbutils: RemoteDbUtils = dbutils

    def get_first_file_or_folder(
        self, path: str, ascending: bool = True
    ) -> tuple[str, bool]:
        files = self._dbutils.fs.ls(path)
        if files:
            files = sorted(
                files, key=lambda f: f.name.lower(), reverse=(not ascending)
            )
            first_file = files[0]
            return (first_file.path, first_file.isDir())
        return None

    def get_folders_list(self, path: str, recursive: bool = False) -> list[str]:
        if recursive:
            # Get all files/folders recursively, then filter for directories
            all_items = []
            
            def _collect_recursive(current_path):
                try:
                    items = self._dbutils.fs.ls(current_path)
                    for item in items:
                        if item.isDir():
                            all_items.append(item.path)
                            _collect_recursive(item.path)
                except Exception:
                    pass
            
            _collect_recursive(path)
            return all_items
        else:
            # Get only immediate subdirectories
            items = self._dbutils.fs.ls(path)
            return [item.path for item in items if item.isDir()]

    def get_files_list(self, path: str, recursive: bool = False) -> list[str]:
        if recursive:
            # Get all files recursively
            all_files = []
            
            def _collect_recursive(current_path):
                try:
                    items = self._dbutils.fs.ls(current_path)
                    for item in items:
                        if item.isDir():
                            _collect_recursive(item.path)
                        else:
                            all_files.append(item.path)
                except Exception:
                    pass
            
            _collect_recursive(path)
            return all_files
        else:
            # Get only immediate files
            items = self._dbutils.fs.ls(path)
            return [item.path for item in items if not item.isDir()]

    def get_file_str_content(self, full_path: str) -> str:
        try:
            # Check if file exists first
            self._dbutils.fs.ls(full_path)
        except Exception:
            # File doesn't exist or path is invalid
            return None

        df = self._spark.read.text(full_path)

        # Collect the rows and join them into a single string
        text_str = "\n".join(row["value"] for row in df.collect())

        return text_str

    def upload_file_str(self, full_path: str, content: str):
        # Use dbutils.fs.put to write content directly to the volume
        self._dbutils.fs.put(full_path, content, overwrite=True)

