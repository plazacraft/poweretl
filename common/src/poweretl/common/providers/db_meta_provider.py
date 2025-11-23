# pylint: disable=R0902, R0801, E1111

import copy
import os
from abc import abstractmethod
from pathlib import Path
from typing import TypeVar

from deepmerge import always_merger  # pylint: disable=C0411
from poweretl.defs import Meta, Model
from poweretl.defs.meta import BaseItem
from poweretl.utils import (
    DataclassUpgrader,
    FileCommandSplitter,
    FileEntry,
    FileMerger,
    MultiFileReader,
    TokensReplacer,
)

from .base_meta_provider import BaseMetaProvider

T = TypeVar("T")


class DbMetaProvider(BaseMetaProvider):
    """Keeps Model metadata and it's provisioning state in database."""

    def __init__(
        self,
        *,
        tables_prefix: str = "workspace.poweretl.",
        schema_paths: list[FileEntry] = [
            FileEntry(
                Path(os.path.dirname(os.path.abspath(__file__)))
                .joinpath("_data")
                .joinpath("db_meta_provider")
                .joinpath("schema"),
                r"\.sql",
            )
        ],
        commands_paths: list[FileEntry] = [
            FileEntry(
                Path(os.path.dirname(os.path.abspath(__file__)))
                .joinpath("_data")
                .joinpath("db_meta_provider"),
                r"commands\.yaml",
            )
        ],
        encoding: str = "utf-8",
    ):  # pylint: disable=R0913
        super().__init__()

        self._file_merger = FileMerger()
        self._commands_splitter = FileCommandSplitter()
        self._tokens_replacer = TokensReplacer()

        self._command_params = {
            "table_meta": f"{tables_prefix}meta",
            "table_version": f"{tables_prefix}version",
        }

        # Load schemas
        self._schema_reader = MultiFileReader(
            file_paths=schema_paths, encoding=encoding
        )

        schema_files = self._schema_reader.get_files_with_content()
        self._schema_contents = [
            (
                command_file,
                self._tokens_replacer.replace(
                    tokens=self._command_params, text=content
                ),
            )
            for command_file, content in schema_files
        ]

        self._schema = self._commands_splitter.get_commands(self._schema_contents)

        # Load commands
        self._commands_reader = MultiFileReader(
            file_paths=commands_paths, encoding=encoding
        )

        command_files = self._commands_reader.get_files_with_content()

        self._commands = self._file_merger.merge(command_files)

    @abstractmethod
    def _execute_command(self, command: str):
        pass

    def _prepare_command(self, command_name: str, params: dict[str, str]) -> str:
        command_template = self._commands.get(command_name)
        if not command_template:
            raise NotImplementedError(f"Command {command_name} not implemented.")
        input_params = copy.deepcopy(self._command_params)
        input_params.update(params)
        return self._tokens_replacer.replace(tokens=input_params, text=command_template)

    def _get_version(self) -> tuple[str, int]:
        command = self._prepare_command("get_version", {})
        result = self._execute_command(command)
        if result and len(result) > 0:
            row = result[0]
            return row["version"], row["step"]
        return "", 0

    def _update_version(self, version: str, step: int):
        command = self._prepare_command(
            "update_version", {"version": version, "step": str(step)}
        )
        self._execute_command(command)

    def self_update(self):
        version, step = self._get_version()
        schema_command = self._commands_splitter.get_commands(
            self._schema_contents, version, step
        )

        for command_entry in schema_command:
            self._execute_command(command_entry.command)
            self._update_version(command_entry.version, command_entry.step)

    def _save_meta(self, meta: Meta):
        pass
        # output_file = None
        # output_dir = ""
        # if not self._store_versions:
        #     output_file = self._file_name
        # else:
        #     output_dir, output_file =
        #          self._file_path_organizer.get_name(datetime.now())
        #     output_file = output_file + self._file_name

        # output_dir = str(Path(self._path).joinpath(output_dir))
        # content = self._file_serializer.to_file_content(output_file, asdict(meta))
        # self._storage_provider.upload_file_str(
        #     str(Path(output_dir).joinpath(output_file).as_posix()), content
        # )

    def push_model_changes(self, model: Model):
        # for file we take always whole file, but for database
        # it would be better to query by tables
        meta = self.get_meta()
        updated_meta = self._get_updated_meta(model, meta)
        self.push_meta_changes(updated_meta)

    def push_meta_changes(self, meta: Meta):
        meta_current_model = self.get_meta()
        meta_merged = always_merger.merge(meta_current_model, meta)
        self._save_meta(meta_merged)

    def push_meta_item_changes(self, item: BaseItem):
        meta_current_model = self.get_meta()
        item_to_update = self._find_by_object_id(meta_current_model, item)
        upgrader = DataclassUpgrader(BaseItem)
        upgrader.update_child(item, item_to_update)
        self._save_meta(meta_current_model)

    def get_meta(self, statuses: set[str] = None, table_id: str = None) -> Meta:
        pass
        # file = self._find_latest_file(self._path)
        # if file:
        #     content = self._storage_provider.get_file_str_content(file)
        #     if content:
        #         meta_dict = self._file_serializer.to_dict(Path(file).name, content)
        #         meta = from_dict(data_class=Meta, data=meta_dict)

        #         if table_id and meta:
        #             meta.tables.items = {
        #                 key: table
        #                 for key, table in meta.tables.items.items()
        #                 if key == table_id
        #             }

        #         if statuses and meta:
        #             meta = self._apply_status_filter(meta, statuses)

        #         return meta
        # # new meta if there is no file found
        # return Meta()
