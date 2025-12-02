# pylint: disable=R0902, R0801, E1111, R0913, W0102, R0917, C0411

import copy
import os
from pathlib import Path
from typing import Any, Optional

from poweretl.utils import (
    FileCommandSplitter,
    FileEntry,
    FileMerger,
    MultiFileReader,
    TokensReplacer,
)
from pyspark.sql import SparkSession


class DbxSchemaReader:

    def __init__(
        self,
        *,
        spark: SparkSession,
        commands_paths: list[FileEntry] = [
            FileEntry(
                Path(os.path.dirname(os.path.abspath(__file__))).joinpath("_data"),
                r"commands\.yaml",
            )
        ],
    ):
        super().__init__()
        self._spark = spark
        self._file_merger = FileMerger()
        self._commands_splitter = FileCommandSplitter()
        self._tokens_replacer = TokensReplacer()

        self._command_params = {}

        # Load commands
        self._commands_reader = MultiFileReader(file_paths=commands_paths)

        command_files = self._commands_reader.get_files_with_content()

        self._commands = self._file_merger.merge(command_files)

    def _execute_command(self, command: str):
        return self._spark.sql(command)

    def _prepare_command(self, command_name: str, params: dict[str, str]) -> str:
        command_template = self._commands.get(command_name)
        if not command_template:
            raise NotImplementedError(f"Command {command_name} not implemented.")
        input_params = copy.deepcopy(self._command_params)
        input_params.update(params)
        return self._tokens_replacer.replace(tokens=input_params, text=command_template)

    def sql_str(self, item: Optional[Any]) -> str:
        if item is None:
            return "NULL"
        if isinstance(item, str):
            return f"'{item.replace('\'', '\'\'')}'"

        if isinstance(item, (int, float)):
            return str(item)
        if isinstance(item, list):
            return f"ARRAY({','.join([self.sql_str(i) for i in item])})"

        return str(item)

    def get_catalogs(self, catalog_name=None, catalog_name_like=None):
        params = {
            "catalog_name": self.sql_str(catalog_name),
            "catalog_name_like": self.sql_str(catalog_name_like),
        }
        command = self._prepare_command("get_catalogs", params)
        return self._execute_command(command)

    def get_schemas(
        self,
        catalog_name=None,
        catalog_name_like=None,
        schema_name=None,
        schema_name_like=None,
    ):
        params = {
            "catalog_name": self.sql_str(catalog_name),
            "catalog_name_like": self.sql_str(catalog_name_like),
            "schema_name": self.sql_str(schema_name),
            "schema_name_like": self.sql_str(schema_name_like),
        }
        command = self._prepare_command("get_schemas", params)
        return self._execute_command(command)

    def get_tables(
        self,
        catalog_name=None,
        catalog_name_like=None,
        schema_name=None,
        schema_name_like=None,
        table_name=None,
        table_name_like=None,
        table_type: list = None,
    ):

        p_type = "(NULL)"
        if (table_type is not None) and (len(table_type) > 0):
            p_type = f"({','.join([self.sql_str(t) for t in table_type])})"

        params = {
            "catalog_name": self.sql_str(catalog_name),
            "catalog_name_like": self.sql_str(catalog_name_like),
            "schema_name": self.sql_str(schema_name),
            "schema_name_like": self.sql_str(schema_name_like),
            "table_name": self.sql_str(table_name),
            "table_name_like": self.sql_str(table_name_like),
            "table_type": p_type,
        }
        command = self._prepare_command("get_tables", params)
        return self._execute_command(command)

    def get_columns(
        self,
        catalog_name,
        schema_name,
        table_name,
        column_name=None,
        column_name_like=None,
    ):

        params = {
            "catalog_name": self.sql_str(catalog_name),
            "schema_name": self.sql_str(schema_name),
            "table_name": self.sql_str(table_name),
            "column_name": self.sql_str(column_name),
            "column_name_like": self.sql_str(column_name_like),
        }
        command = self._prepare_command("get_columns", params)
        return self._execute_command(command)
