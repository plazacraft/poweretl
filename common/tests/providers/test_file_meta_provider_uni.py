# pylint: disable=W0212, W0613

import copy
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pytest
from poweretl.defs import Meta, Model, Operation, Status

from poweretl.common import FileMetaProvider


class DummyStorageProvider:
    """A fake storage provider that stores files in-memory keyed by path."""

    def __init__(self):
        # keys are directory paths, values are dicts mapping filename -> content
        self._store = {}

    def get_first_file_or_folder(self, path, _):
        # return the first entry in the directory or None
        dir_contents = self._store.get(path, {})
        if not dir_contents:
            return None, False
        # pick latest by name ordering to simulate versioned dirs
        first = sorted(dir_contents.keys())[-1]
        value = dir_contents[first]
        # if value is a dict, it's a folder; else a file
        if isinstance(value, dict):
            return Path(path).joinpath(first).as_posix(), True
        return Path(path).joinpath(first).as_posix(), False

    def upload_file_str(self, output_file, content):
        output_dir = Path(output_file).parent.as_posix()
        filename = Path(output_file).name
        self._store.setdefault(output_dir, {})[filename] = content

    def get_file_str_content(self, file):
        # file is a path to the file; return its content
        p = Path(file)
        dirpath = p.parent.as_posix()
        filename = p.name
        return self._store.get(dirpath, {}).get(filename)


class DummySerializer:
    def to_file_content(self, filename, data):
        # filename may be a Path; serialize dict to JSON string
        return json.dumps(data)

    def to_dict(self, filename, content):
        return json.loads(content)


def make_simple_meta():
    # Create a minimal Meta dataclass instance
    m = Meta()
    m.tables = {}
    return m


def make_simple_model():
    model = Model()
    model.tables = {}
    return model


def test_save_meta_without_versions_creates_single_file():
    storage = DummyStorageProvider()
    serializer = DummySerializer()

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/meta",
        store_versions=False,
        storage_provider=storage,
        file_serializer=serializer,
    )

    meta = make_simple_meta()

    provider._save_meta(meta)

    # since store_versions is False, file should be at /tmp/meta/metadata.json
    content = storage.get_file_str_content(Path("/tmp/meta/metadata.json"))
    assert content is not None
    loaded = json.loads(content)
    assert loaded == asdict(meta)


def test_save_meta_with_versions_creates_versioned_dir_and_file():
    storage = DummyStorageProvider()
    serializer = DummySerializer()

    class FixedOrganizer:
        def get_name(self, _dt: datetime):
            return ("ver-1", "prefix_")

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/metav",
        store_versions=True,
        storage_provider=storage,
        file_serializer=serializer,
        file_path_organizer=FixedOrganizer(),
    )

    meta = make_simple_meta()

    provider._save_meta(meta)

    content = storage.get_file_str_content(
        Path("/tmp/metav/ver-1/prefix_metadata.json")
    )
    assert content is not None
    loaded = json.loads(content)
    assert loaded == asdict(meta)


def test_find_latest_file_when_no_versions_returns_filename():
    storage = DummyStorageProvider()
    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/nover",
        store_versions=False,
        storage_provider=storage,
    )

    assert str(Path(provider._find_latest_file("/tmp/nover"))) == str(
        Path("/tmp/nover/metadata.json")
    )


def test_find_latest_file_with_versioned_structure():
    storage = DummyStorageProvider()
    # simulate structure: /tmp/v -> {'20250101': {'meta': {'metadata.json': '...'}}}
    storage._store["/tmp/v"] = {"20250101": {}}
    storage._store["/tmp/v/20250101"] = {"meta": {}}
    storage._store["/tmp/v/20250101/meta"] = {"metadata.json": json.dumps({"id": "x"})}

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/v",
        store_versions=True,
        storage_provider=storage,
    )

    latest = provider._find_latest_file("/tmp/v")
    assert latest is not None
    # should point to the file path
    assert latest.endswith("metadata.json")


def test_get_meta_returns_meta_object(monkeypatch):
    storage = DummyStorageProvider()
    serializer = DummySerializer()

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/getmeta",
        store_versions=True,
        storage_provider=storage,
        file_serializer=serializer,
    )

    # create a stored meta
    meta = {"tables": {"prune": True}}
    storage._store["/tmp/getmeta/ver/meta"] = {"metadata.json": json.dumps(meta)}

    # make get_first_file_or_folder return the path to the file
    def fake_get_first(path, _):
        return ("/tmp/getmeta/ver/meta/metadata.json", False)

    provider._storage_provider.get_first_file_or_folder = fake_get_first

    result = provider.get_meta()
    # from_dict should construct Meta; ensure id is present
    assert result is not None
    assert getattr(result.tables, "prune")


@pytest.mark.parametrize("table_id", [None, "t1"])
def test_get_meta_with_table_id_filters(monkeypatch, table_id):
    storage = DummyStorageProvider()
    serializer = DummySerializer()

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/filter",
        store_versions=False,
        storage_provider=storage,
        file_serializer=serializer,
    )

    # store content at /tmp/filter/metadata.json

    meta = {
        "tables": {
            "items": {
                "t1": {
                    "name": "t1",
                    "meta": {
                        "object_id": "1",
                        "operation": Operation.NEW.value,
                        "status": Status.SUCCESS.value,
                    },
                },
                "t2": {
                    "name": "t2",
                    "meta": {
                        "object_id": "2",
                        "operation": Operation.NEW.value,
                        "status": Status.SUCCESS.value,
                    },
                },
            }
        }
    }
    storage._store["/tmp/filter"] = {"metadata.json": json.dumps(meta)}

    # monkeypatch get_first_file_or_folder to return the file path
    provider._storage_provider.get_first_file_or_folder = lambda p, _: (
        "/tmp/filter/metadata.json",
        False,
    )

    result = provider.get_meta(table_id=table_id)
    assert result is not None
    if table_id is None:
        assert isinstance(result.tables.items, dict)
        assert len(result.tables.items) == 2
    else:
        # expecting only the matching table in result.tables
        assert len(result.tables.items) == 1
        assert result.tables.items[table_id].name == table_id


def test_get_meta_filters_by_status(monkeypatch):
    """Ensure get_meta(status=...) returns only branches that contain
    at least one BaseItem with matching status anywhere in their subtree.

    We construct a meta with two tables. Table t1 has two columns where
    column c1 has status FAILED and c2 SUCCESS. Table t2 has no FAILED
    status anywhere. Expect the result to keep only t1 and only column c1.
    """
    storage = DummyStorageProvider()
    serializer = DummySerializer()

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/status",
        store_versions=False,
        storage_provider=storage,
        file_serializer=serializer,
    )

    # Build meta with nested columns
    meta = {
        "tables": {
            "items": {
                "t1": {
                    "name": "t1",
                    "meta": {
                        "object_id": "1",
                        "operation": Operation.NEW.value,
                        "status": Status.SUCCESS.value,
                    },
                    "columns": {
                        "items": {
                            "c1": {
                                "name": "c1",
                                "meta": {
                                    "object_id": "c1",
                                    "operation": Operation.NEW.value,
                                    "status": Status.FAILED.value,
                                },
                            },
                            "c2": {
                                "name": "c2",
                                "meta": {
                                    "object_id": "c2",
                                    "operation": Operation.NEW.value,
                                    "status": Status.SUCCESS.value,
                                },
                            },
                        }
                    },
                },
                "t2": {
                    "name": "t2",
                    "meta": {
                        "object_id": "2",
                        "operation": Operation.NEW.value,
                        "status": Status.SUCCESS.value,
                    },
                    "columns": {"items": {}},
                },
            }
        }
    }

    # Store the JSON at the expected path
    storage._store["/tmp/status"] = {"metadata.json": json.dumps(meta)}

    # Monkeypatch provider to return the file path
    provider._storage_provider.get_first_file_or_folder = lambda p, _: (
        "/tmp/status/metadata.json",
        False,
    )

    # Request only FAILED items
    result = provider.get_meta(status=Status.FAILED.value)

    # Expect only t1 to remain
    assert result is not None
    assert isinstance(result.tables.items, dict)
    assert "t1" in result.tables.items
    assert "t2" not in result.tables.items

    # Inside t1, expect only column c1 to remain (the FAILED one)
    t1 = result.tables.items["t1"]
    assert hasattr(t1, "columns")
    assert isinstance(t1.columns.items, dict)
    assert "c1" in t1.columns.items
    assert "c2" not in t1.columns.items


def test_push_meta_item_changes_updates_item():
    """Ensure push_meta_item_changes updates a single item in stored meta.

    We store a meta with one table 't1'. We then fetch that table object,
    modify its `name` and call `push_meta_item_changes` with the modified
    dataclass. The provider should save the updated meta with the changed
    table name.
    """

    storage = DummyStorageProvider()
    serializer = DummySerializer()

    provider = FileMetaProvider(
        file_name="metadata.json",
        path="/tmp/update",
        store_versions=False,
        storage_provider=storage,
        file_serializer=serializer,
    )

    # initial meta with one table t1
    meta = {
        "tables": {
            "items": {
                "t1": {
                    "name": "t1",
                    "meta": {
                        "object_id": "1",
                        "operation": Operation.NEW.value,
                        "status": Status.SUCCESS.value,
                    },
                }
            }
        }
    }
    storage._store["/tmp/update"] = {"metadata.json": json.dumps(meta)}

    # load current meta as dataclass and prepare modified item
    current = provider.get_meta()
    assert "t1" in current.tables.items
    item = copy.deepcopy(current.tables.items["t1"])
    # change a property
    item.name = "t1-renamed"

    # push change
    provider.push_meta_item_changes(item)

    # read saved content and verify change persisted
    saved = storage.get_file_str_content(Path("/tmp/update/metadata.json"))
    assert saved is not None
    saved_dict = json.loads(saved)
    assert saved_dict["tables"]["items"]["t1"]["name"] == "t1-renamed"
