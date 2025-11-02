# pylint: disable=R0911

import os

from poweretl.utils import FileEntry, OSFileStorageProvider

from poweretl.common import FileMetaProvider, FileModelProvider

from poweretl.utils.tests import deep_compare, deep_compare_true

class CacheStorageProvider(OSFileStorageProvider):
    """A fake storage provider that stores files in-memory keyed by path."""

    def __init__(self, use_cache: bool = False):
        super().__init__()
        self._use_cache = use_cache
        self._cache = None

    def get_file_str_content(self, full_path):
        if (self._use_cache) and (self._cache is not None):
            return self._cache
        return super().get_file_str_content(full_path)

    def upload_file_str(self, full_path: str, content: str):
        if self._use_cache:
            self._cache = content
        else:
            super().upload_file_str(full_path, content)



def test_file_meta_provider_int():

    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data/meta"
    result_dir = f"{module_dir}/_data/result"

    meta_provider_init = FileMetaProvider(
        file_name="meta_init.json",
        path=data_dir,
    )

    meta_provider_result = FileMetaProvider(
        file_name="meta.json",
        path=result_dir,
    )

    meta_provider_current = FileMetaProvider(
        file_name="meta_current.json",
        path=result_dir,
        storage_provider=CacheStorageProvider(use_cache=True),
    )

    model_provider_init = FileModelProvider(
        config_paths=[FileEntry(data_dir, r"model_init\.json?$")],
    )

    model_provider_update = FileModelProvider(
        config_paths=[FileEntry(data_dir, r"model_update\.json?$")],
    )

    model_init = model_provider_init.get_model()
    # meta_provider_init.push_model_changes(model_init)
    meta_provider_current.push_model_changes(model_init)

    meta_init = meta_provider_init.get_meta()
    meta_current = meta_provider_current.get_meta()

    exclude = ["object_id", "model_last_update", "meta_last_update"]

    assert deep_compare(
        meta_init,
        meta_current,
        exclude=exclude,
    ), "Initial meta does not match"

    model_update = model_provider_update.get_model()

    # meta_provider_result.push_model_changes(model_init)
    # meta_provider_result.push_model_changes(model_update)

    meta_provider_current.push_model_changes(model_update)
    meta_result = meta_provider_result.get_meta()
    meta_current = meta_provider_current.get_meta()

    assert deep_compare(meta_result, meta_current, exclude=exclude)
    assert deep_compare_true(meta_result, meta_current, exclude=exclude)
