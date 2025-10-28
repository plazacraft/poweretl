# pylint: disable=protected-access

from pathlib import Path

from poweretl.utils.providers.mem_file_storage_provider import MemFileStorageProvider

from poweretl.common.managers.base_model_manager import BaseModelManager
from poweretl.common.providers.file_meta_provider import FileMetaProvider


class CapturingModelManager(BaseModelManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands: list[str] = []

    def _execute_command(self, command: str):
        # store executed SQL for later comparison
        self.commands.append(command.rstrip("\n"))


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
