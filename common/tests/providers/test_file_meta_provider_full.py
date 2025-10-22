import os

from poweretl.utils import FileEntry, OSFileStorageProvider

from poweretl.common import FileMetaProvider, FileModelProvider


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


def deep_compare(a, b, exclude=None):
    exclude = set(exclude or [])
    if isinstance(a, dict) and isinstance(b, dict):
        return all(
            k in b and deep_compare(v, b[k], exclude)
            for k, v in a.items()
            if k not in exclude
        )
    if isinstance(a, list) and isinstance(b, list):
        return all(deep_compare(x, y, exclude) for x, y in zip(a, b))
    if hasattr(a, "__dict__") and hasattr(b, "__dict__"):
        return deep_compare(vars(a), vars(b), exclude)

    return a == b


def test_file_meta_provider_full():

    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data/meta"
    result_dir = f"{module_dir}/_data/result"

    meta_provider_init = FileMetaProvider(
        file_name="meta_init.json",
        path=data_dir,
    )

    # meta_provider_result = FileMetaProvider(
    #     file_name="meta.json",
    #     path=result_dir,
    # )

    meta_provider_current = FileMetaProvider(
        file_name="meta_current.json",
        path=result_dir,
        storage_provider=CacheStorageProvider(use_cache=True),
    )

    model_provider_init = FileModelProvider(
        config_paths=[FileEntry(data_dir, r"model_init\.json?$")],
    )

    # model_provider_update = FileModelProvider(
    #     config_paths=[FileEntry(data_dir, r"model_update\.json?$")],
    # )

    model_init = model_provider_init.get_model()
    meta_provider_current.push_model_changes(model_init)

    meta_init = meta_provider_init.get_meta()
    meta_current = meta_provider_current.get_meta()

    assert deep_compare(
        meta_init,
        meta_current,
        exclude=["object_id"],
    ), "Initial meta does not match"
