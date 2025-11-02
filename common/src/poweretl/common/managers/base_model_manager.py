# pylint: disable=R0912, W0718

import os
from abc import abstractmethod
from datetime import datetime
from pathlib import Path

from poweretl.defs import IMetaProvider, IModelManager
from poweretl.defs.meta import BaseItem, Operation, Status
from poweretl.utils import FileEntry, FileMerger, MultiFileReader, TokensReplacer


class BaseModelManager(IModelManager):

    def __init__(
        self,
        meta_provider: IMetaProvider,
        config_paths: list[FileEntry] = [
            FileEntry(
                Path(os.path.dirname(os.path.abspath(__file__))).joinpath("_data"),
                r"sql_model_manager\.yaml",
            )
        ],
        encoding: str = "utf-8",
    ):
        self._meta_provider = meta_provider
        self._config_reader = MultiFileReader(
            file_paths=config_paths, encoding=encoding
        )
        self._file_merger = FileMerger()

        self._config = self._file_merger.merge(
            self._config_reader.get_files_with_content()
        )
        self._tokens_replacer = TokensReplacer()

    @abstractmethod
    def _execute_command(self, command: str):
        pass

    def _call_execute_command(self, command: str, item: BaseItem, **kwargs):
        try:
            commands = {}
            # Expand UPDATED only for structural alters that map
            # updated_fields to specific commands
            if item.meta.operation == Operation.UPDATED.value and command in (
                "alter_table",
                "alter_column",
            ):
                # For each updated field, create a command like
                # "alter_table_name", "alter_table_comment", etc.
                commands = {
                    f"{command}_{updated}": getattr(item, updated, None)
                    for updated in (item.meta.updated_fields or [])
                }
            else:
                # Execute the provided command as-is
                # (NEW/DELETED or specialized UPDATED like tag/property value)
                commands = {command: None}

            item.meta.status = Status.RUNNING.value
            self._meta_provider.push_meta_item_changes(item)

            for current_command, value in commands.items():
                if current_command not in self._config:
                    raise NotImplementedError(
                        f"Command not implemented: {current_command}"
                    )

                query = self._config[current_command]
                # Build tokens dict (stringified, ignore None).
                # If value is provided for the command, include it.
                tokens = {k: str(v) for k, v in kwargs.items() if v is not None}
                if value is not None:
                    tokens["value"] = str(value)
                query = self._tokens_replacer.replace(query, tokens=tokens)
                self._execute_command(query)

            item.meta.status = Status.SUCCESS.value
            item.meta.meta_last_update = datetime.now().isoformat()
            self._meta_provider.push_meta_item_changes(item)

        except Exception as e:
            item.meta.status = Status.FAILED.value
            item.meta.meta_last_update = datetime.now().isoformat()
            item.meta.error_msg = str(e)
            self._meta_provider.push_meta_item_changes(item)

    def provision_model(self, table_id: str = None):
        meta = self._meta_provider.get_meta(table_id = table_id, status={Status.PENDING.value})
        for table in meta.tables.items.values():
            if table.meta.operation == Operation.NEW.value:
                external_clause = ""
                location_clause = ""
                if table.external_location:
                    external_clause = "EXTERNAL"
                    location_clause = f"LOCATION '{table.external_location}'"
                comment_clause = ""
                if table.comment:
                    comment_clause = f"COMMENT '{table.comment}'"
                cluster_by_clause = ""
                if table.cluster_by:
                    cluster_by_clause = f"CLUSTER BY {table.cluster_by}"
                self._call_execute_command(
                    "create_table",
                    table,
                    table_name=table.name,
                    external_clause=external_clause,
                    location_clause=location_clause,
                    comment_clause=comment_clause,
                    cluster_by_clause=cluster_by_clause,
                )
            elif table.meta.operation == Operation.UPDATED.value:
                self._call_execute_command("alter_table", table, table_name=table.name)

            elif table.meta.operation == Operation.DELETED.value:
                self._call_execute_command("drop_table", table, table_name=table.name)

            # Process columns
            for column in table.columns.items.values():
                if column.meta.operation == Operation.NEW.value:
                    comment_clause = ""
                    if hasattr(column, "comment") and column.comment:
                        comment_clause = f"COMMENT '{column.comment}'"
                    self._call_execute_command(
                        "create_column",
                        column,
                        table_name=table.name,
                        column_name=column.name,
                        column_type=column.type if column.type else "",
                        comment_clause=comment_clause,
                    )
                elif column.meta.operation == Operation.UPDATED.value:
                    self._call_execute_command(
                        "alter_column",
                        column,
                        table_name=table.name,
                        column_name=column.name,
                    )
                elif column.meta.operation == Operation.DELETED.value:
                    self._call_execute_command(
                        "drop_column",
                        column,
                        table_name=table.name,
                        column_name=column.name,
                    )

                # Process column tags
                for column_tag in column.tags.items.values():
                    if column_tag.meta.operation == Operation.NEW.value:
                        self._call_execute_command(
                            "create_column_tag",
                            column_tag,
                            table_name=table.name,
                            column_name=column.name,
                            tag_name=column_tag.name,
                            tag_value=column_tag.value if column_tag.value else "",
                        )
                    elif column_tag.meta.operation == Operation.UPDATED.value:
                        self._call_execute_command(
                            "alter_column_tag_value",
                            column_tag,
                            table_name=table.name,
                            column_name=column.name,
                            tag_name=column_tag.name,
                            value=column_tag.value,
                        )
                    elif column_tag.meta.operation == Operation.DELETED.value:
                        self._call_execute_command(
                            "drop_column_tag",
                            column_tag,
                            table_name=table.name,
                            column_name=column.name,
                            tag_name=column_tag.name,
                        )

            # Process table tags
            for tag in table.tags.items.values():
                if tag.meta.operation == Operation.NEW.value:
                    self._call_execute_command(
                        "create_table_tag",
                        tag,
                        table_name=table.name,
                        tag_name=tag.name,
                        tag_value=tag.value if tag.value else "",
                    )
                elif tag.meta.operation == Operation.UPDATED.value:
                    self._call_execute_command(
                        "alter_table_tag_value",
                        tag,
                        table_name=table.name,
                        tag_name=tag.name,
                        value=tag.value,
                    )
                elif tag.meta.operation == Operation.DELETED.value:
                    self._call_execute_command(
                        "drop_table_tag", tag, table_name=table.name, tag_name=tag.name
                    )

            # Process table properties
            for current_property in table.properties.items.values():
                if current_property.meta.operation == Operation.NEW.value:
                    self._call_execute_command(
                        "create_table_property",
                        current_property,
                        table_name=table.name,
                        property_name=current_property.name,
                        property_value=(
                            current_property.value if current_property.value else ""
                        ),
                    )
                elif current_property.meta.operation == Operation.UPDATED.value:
                    self._call_execute_command(
                        "alter_table_property_value",
                        current_property,
                        table_name=table.name,
                        property_name=current_property.name,
                        value=current_property.value,
                    )
                elif current_property.meta.operation == Operation.DELETED.value:
                    self._call_execute_command(
                        "drop_table_property",
                        current_property,
                        table_name=table.name,
                        property_name=current_property.name,
                    )
