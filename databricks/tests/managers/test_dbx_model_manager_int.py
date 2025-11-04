import os
from pathlib import Path
import pytest

from poweretl.common import FileMetaProvider
from poweretl.utils import MemFileStorageProvider
from poweretl.databricks.managers import DbxModelManager
from poweretl.databricks.helpers import get_or_connect
from pyspark.sql import SparkSession
from poweretl.utils.tests import deep_compare, deep_compare_true


@pytest.fixture(scope="function")
def test_data_setup():
    """Setup test data and return storage provider with loaded files."""
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
        Path(f"{data_dir}/02.meta_update.json"), "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(Path(mem_path).joinpath("meta_update.json").as_posix(), f.read())

    with open(
        Path(f"{data_dir}/03.meta_cleanup.json"), "r", encoding="utf-8"
    ) as f:
        storage.upload_file_str(Path(mem_path).joinpath("meta_cleanup.json").as_posix(), f.read())

    return {
        "storage": storage,
        "mem_path": mem_path,
        "data_dir": data_dir,
        "catalog": catalog,
        "schema": schema
    }


@pytest.fixture(scope="function")
def spark_session():
    """Provide Spark session."""
    return get_or_connect().spark


@pytest.fixture(scope="function")
def cleanup_manager(test_data_setup, spark_session):
    """Setup cleanup manager that will run after test completion."""
    # Create cleanup meta provider
    meta_provider_cleanup = FileMetaProvider(
        file_name="meta_cleanup.json",
        path=test_data_setup["mem_path"],
        store_versions=False,
        storage_provider=test_data_setup["storage"],
    )

    meta_provider_cleanup_result = FileMetaProvider(
        file_name="03.meta_cleanup_results.json",  # Fixed filename (was missing 's')
        path=test_data_setup["data_dir"],
        store_versions=False,
    )

    cleanup_mgr = DbxModelManager(spark=spark_session, meta_provider=meta_provider_cleanup)
    
    yield cleanup_mgr
    
    cleanup_mgr.provision_model()

    cleanup_results = meta_provider_cleanup.get_meta()
    expected_cleanup_results = meta_provider_cleanup_result.get_meta()
    exclude = ["model_last_update", "meta_last_update"]       
    assert deep_compare_true(cleanup_results, expected_cleanup_results, exclude=exclude), \
        f"Cleanup results do not match expected"



def test_dbx_model_manager(test_data_setup, spark_session, cleanup_manager):
    """Test DbxModelManager init and update operations."""
    storage = test_data_setup["storage"]
    mem_path = test_data_setup["mem_path"]
    data_dir = test_data_setup["data_dir"]

    try:
        # Verify cleanup manager is available (cleanup will run in fixture teardown)
        assert cleanup_manager is not None

        meta_provider_init = FileMetaProvider(
            file_name="meta_init.json",
            path=mem_path,
            store_versions=False,
            storage_provider=storage,
        )
        meta_provider_update = FileMetaProvider(
            file_name="meta_update.json",
            path=mem_path,
            store_versions=False,
            storage_provider=storage,
        )

        meta_provider_init_result = FileMetaProvider(
            file_name="01.meta_init_result.json",
            path=data_dir,
            store_versions=False,
        )

        meta_provider_update_result = FileMetaProvider(
            file_name="02.meta_update_result.json",
            path=data_dir,
            store_versions=False,
        )

        # Test init
        mgr_init = DbxModelManager(spark=spark_session, meta_provider=meta_provider_init)
        mgr_init.provision_model()
        init_results = meta_provider_init.get_meta()
        expected_init_results = meta_provider_init_result.get_meta()
        exclude = ["model_last_update", "meta_last_update"]
        assert deep_compare_true(init_results, expected_init_results, exclude=exclude), \
            f"Init results do not match expected"

        # Test update
        mgr_update = DbxModelManager(spark=spark_session, meta_provider=meta_provider_update)
        mgr_update.provision_model()
        update_results = meta_provider_update.get_meta()
        expected_update_results = meta_provider_update_result.get_meta()
        exclude = ["model_last_update", "meta_last_update"]
        assert deep_compare_true(update_results, expected_update_results, exclude=exclude), \
            f"Update results do not match expected"

    
    except Exception as e:
        print(f"Test failed with error: {e}")
        # Re-raise the exception so pytest reports the failure
        # The cleanup will still run in the fixture teardown
        raise
