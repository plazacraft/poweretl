# pylint: disable=protected-access, W0621, R0914
import os
from pathlib import Path

from poweretl.common import FileMetaProvider, FileModelProvider
from poweretl.utils.tests import deep_compare_true
from poweretl.utils.file import MultiFileReader
from poweretl.utils import FileEntry
from poweretl.databricks.helpers import get_or_connect
from poweretl.databricks.managers import DbxModelManager
from poweretl.databricks.providers import DbxVolumeFileStorageProvider


def run_cleanup(spark, dbutils, env):
    model_manager = get_manager(
        spark=spark,
        dbutils=dbutils,
        env=env,
        params_path="_model_definition",
        model_path="_model_cleanup"
    )

    model_manager.provision_model()


def get_manager(spark, dbutils, env, params_path, model_path):
    module_dir = os.path.dirname(os.path.abspath(__file__))
    model_dir = f"{module_dir}/{model_path}"
    params_dir = f"{module_dir}/{params_path}"

    # if run from databricks, spark and dbutils will be provided and it takes priority over config file
    # if these are not provided, we connect using config file
    config_path = f"{module_dir}/../../.databricks.config.json"
    session = get_or_connect(spark=spark, dbutils=dbutils, config_path=config_path)


    # Load models from json files. Load parameters based on env selected
    model_provider = FileModelProvider(
        config_paths=[FileEntry(f"{model_dir}", r"\.jsonc?$")],
        param_paths=[FileEntry(f"{params_dir}", f"(01\\.global|01\\.{env})\\.yaml$")],
    )

    # Get schema and catalog from parameters
    catalog = model_provider.params["catalog"]
    schema = model_provider.params["schema"]

    # Meta is kept in Databricks Volume
    volume_storage_provider = DbxVolumeFileStorageProvider(
        session.spark, session.dbutils)


    # Meta is kept as a file using volume provider
    volume_path = f"/Volumes/{catalog}/{schema}/meta/state"
    meta_provider = FileMetaProvider(
        file_name="meta.json",
        path=volume_path,
        store_versions=False,
        storage_provider=volume_storage_provider,
    )

    # Create model manager that works with target meta provider
    model_manager = DbxModelManager(
        spark=spark, meta_provider=meta_provider
    )

    return model_manager

def test_dbx_model_manager(
        spark = None, 
        dbutils = None, 
        env = "tst", 
        run_cleanup = True):

    model_manager = get_manager(
        spark=spark,
        dbutils=dbutils,
        env=env,
        params_path="_model_definition",
        model_path="_model_definition"
    )

    was_exception = False
    try:
        # All the magic here - Provision model
        model_manager.provision_model()

    except Exception as e:
        print(f"Test failed with error: {e}")
        was_exception = True
        raise
    finally:
        # Do cleanup if required
        if run_cleanup or was_exception:
            run_cleanup(spark, dbutils, env)