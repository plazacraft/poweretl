import os
from pathlib import Path

from poweretl.common import FileMetaProvider
from poweretl.utils import MemFileStorageProvider
from poweretl.databricks.managers import DbxModelManager
from poweretl.databricks.helpers import get_or_connect
from pyspark.sql import SparkSession
from poweretl.utils.tests import deep_compare, deep_compare_true


def test_dbx_model_manager():
    catalog = "workspace"
    schema = "poweretl_tests"

    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_data"


    # Prepare in-memory storage with meta.json
    storage = MemFileStorageProvider()
    mem_path = "/tmp/manager"

    with open(
        Path(f"{data_dir}/01.meta_init.json"), "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(Path(mem_path).joinpath("meta_init.json").as_posix(), f.read())

    with open(
        Path(f"{data_dir}/03.meta_cleanup.json"), "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(Path(mem_path).joinpath("meta_cleanup.json").as_posix(), f.read())

    meta_provider_init = FileMetaProvider(
        file_name="meta_init.json",
        path=mem_path,
        store_versions=False,
        storage_provider=storage,
    )

    meta_provider_init_result = FileMetaProvider(
        file_name="01.meta_init_result.json",
        path=data_dir,
        store_versions=False,
    )

    meta_provider_cleanup = FileMetaProvider(
        file_name="meta_cleanup.json",
        path=mem_path,
        store_versions=False,
        storage_provider=storage,
    )

    spark = get_or_connect().spark

    mgr_init = DbxModelManager(spark=spark, meta_provider=meta_provider_init)
    mgr_init.provision_model()

    init_results = meta_provider_init.get_meta()
    expected_init_results = meta_provider_init_result.get_meta()
    exclude = ["model_last_update", "meta_last_update"]    

    assert deep_compare_true(init_results, expected_init_results, exclude=exclude), \
        f"Init results do not match expected"
    
    mgr_cleanup = DbxModelManager(spark=spark, meta_provider=meta_provider_cleanup)
    mgr_cleanup.provision_model()
