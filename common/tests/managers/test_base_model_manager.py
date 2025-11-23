# pylint: disable=protected-access
# Mostly AI

from pathlib import Path

from poweretl.utils.providers.mem_file_storage_provider import MemFileStorageProvider

from poweretl.common.managers.base_model_manager import BaseModelManager
from poweretl.common.providers.file_meta_provider import FileMetaProvider
from poweretl.defs.model import Table, Column, Columns, NameValue, NameValues


class CapturingModelManager(BaseModelManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands: list[str] = []
        self.source_tables: dict[str, Table] = {}

    def _execute_command(self, command: str):
        # store executed SQL for later comparison
        self.commands.append(command.rstrip("\n"))

    def get_table_model_from_source(self, table_name: str) -> Table:
        # Return the table from source_tables if exists, otherwise None
        return self.source_tables.get(table_name, None)


def test_base_model_manager_executes_expected_commands():
    # Prepare in-memory storage with meta.json
    storage = MemFileStorageProvider()
    data_root = "/tmp/manager"
    meta_path = Path(data_root) / "metadata.json"

    with open(
        Path(__file__).parent / "_data" / "meta.json", "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(meta_path.as_posix(), f.read())

    provider = FileMetaProvider(
        file_name="metadata.json",
        path=data_root,
        store_versions=False,
        storage_provider=storage,
    )

    mgr = CapturingModelManager(meta_provider=provider)
    mgr.provision_model()

    # Load expected commands
    expected_file = Path(__file__).parent / "_data" / "expected_commands.txt"
    expected = [
        line.rstrip("\n")
        for line in expected_file.read_text(encoding="utf-8").splitlines()
        if line.strip() != ""
    ]

    # with open("output.txt", "w", encoding="utf-8") as f:
    #     for line in mgr.commands:
    #         f.write(line + "\n")

    # Compare lists exactly
    assert mgr.commands == expected


def test_sync_meta_updates_table_from_source():
    """Test that sync_meta correctly updates meta with data from source."""
    # Prepare in-memory storage with meta.json
    storage = MemFileStorageProvider()
    data_root = "/tmp/manager"
    meta_path = Path(data_root) / "metadata.json"

    with open(
        Path(__file__).parent / "_data" / "meta.json", "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(meta_path.as_posix(), f.read())

    provider = FileMetaProvider(
        file_name="metadata.json",
        path=data_root,
        store_versions=False,
        storage_provider=storage,
    )

    mgr = CapturingModelManager(meta_provider=provider)

    # Create a source table model with updated values
    source_table = Table(
        name="t_upd",
        external_location="/mnt/external/t_upd_synced",
        comment="table synced from source",
        columns=Columns(
            items={
                "c_upd": Column(
                    name="c_upd",
                    type="STRING",
                    comment="synced comment from source"
                ),
                "c_new_from_source": Column(
                    name="c_new_from_source",
                    type="BIGINT",
                    comment="new column from source"
                )
            }
        ),
        tags=NameValues(
            items={
                "tag_upd": NameValue(name="tag_upd", value="tv2_synced"),
                "tag_new_from_source": NameValue(name="tag_new_from_source", value="tv_source")
            }
        )
    )

    # Set the source table for the manager
    mgr.source_tables["t_upd"] = source_table

    # Get meta before sync
    meta_before = provider.get_meta(table_id="t_upd")
    table_before = meta_before.tables.items["t_upd"]
    
    # Verify initial state
    assert table_before.external_location == "/mnt/external/t_upd_new"
    assert table_before.comment == "table upd"
    assert table_before.meta.status == "pending"
    assert table_before.meta.operation == "updated"

    # Run sync_meta
    mgr.sync_meta(table_id="t_upd")

    # Get meta after sync
    meta_after = provider.get_meta(table_id="t_upd")
    table_after = meta_after.tables.items["t_upd"]

    # Verify table was synced with source values
    assert table_after.external_location == "/mnt/external/t_upd_synced"
    assert table_after.comment == "table synced from source"
    assert table_after.meta.status == "success"
    assert table_after.meta.operation == "updated"
    assert table_after.meta.error_msg is None

    # Verify column was synced
    assert "c_upd" in table_after.columns.items
    assert table_after.columns.items["c_upd"].comment == "synced comment from source"

    # Verify tag was synced
    assert "tag_upd" in table_after.tags.items
    assert table_after.tags.items["tag_upd"].value == "tv2_synced"


def test_sync_meta_marks_deleted_when_source_returns_none():
    """Test that sync_meta marks table as deleted when source returns None."""
    # Prepare in-memory storage with meta.json
    storage = MemFileStorageProvider()
    data_root = "/tmp/manager"
    meta_path = Path(data_root) / "metadata.json"

    with open(
        Path(__file__).parent / "_data" / "meta.json", "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(meta_path.as_posix(), f.read())

    provider = FileMetaProvider(
        file_name="metadata.json",
        path=data_root,
        store_versions=False,
        storage_provider=storage,
    )

    mgr = CapturingModelManager(meta_provider=provider)

    # Don't set any source table, so get_table_model_from_source will return None
    # Get meta before sync
    meta_before = provider.get_meta(table_id="t_upd")
    table_before = meta_before.tables.items["t_upd"]
    
    # Verify initial state
    assert table_before.meta.status == "pending"
    assert table_before.meta.operation == "updated"

    # Run sync_meta
    mgr.sync_meta(table_id="t_upd")

    # Get meta after sync
    meta_after = provider.get_meta(table_id="t_upd")
    table_after = meta_after.tables.items["t_upd"]

    # Verify table was marked as deleted
    assert table_after.meta.status == "success"
    assert table_after.meta.operation == "deleted"
    assert table_after.meta.error_msg is None

