# pylint: disable=protected-access, W0621, R0914
import os
from pathlib import Path

from poweretl.common import FileMetaProvider, FileModelProvider
from poweretl.utils.tests import deep_compare_true
from poweretl.utils.file import MultiFileReader
from poweretl.utils.file import FileEntry
from poweretl.utils.providers import MemFileStorageProvider
from poweretl.databricks.helpers import get_or_connect
from poweretl.databricks.managers import DbxModelManager
from poweretl.databricks.providers import DbxVolumeFileStorageProvider


def run_cleanup(spark, dbutils, env):
    model_manager, volume_path, session, volume_storage_provider, mem_storage_provider = get_manager(
        spark=spark,
        dbutils=dbutils,
        env=env,
        params_path="_model_definition",
        model_path="_model_cleanup"
    )


    model_manager.provision_model()
    meta_file = Path(volume_path).joinpath("meta.json").as_posix()
    volume_storage_provider.upload_file_str(meta_file,mem_storage_provider.get_file_str_content(meta_file))
    session.dbutils.fs.rm(volume_path, True)


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
        param_paths=[FileEntry(f"{params_dir}", f"(global|{env})\\.yaml$")],
    )

    # Get schema and catalog from parameters
    catalog = model_provider.params["catalog"]
    schema = model_provider.params["schema"]

    # Meta is kept in Databricks Volume
    volume_storage_provider = DbxVolumeFileStorageProvider(
        session.spark, session.dbutils)


    # Meta is kept as a file using volume provider
    volume_path = f"/Volumes/{catalog}/{schema}/meta/state"
    volume_full_path = str(Path(volume_path).joinpath("meta.json").as_posix())

    meta_provider = FileMetaProvider(
        file_name="meta.json",
        path=volume_path,
        store_versions=False,
        storage_provider=volume_storage_provider,
    )

    # Update model in meta
    meta_provider.push_model_changes(model_provider.get_model())


    # Meta provider writes to storage on each change, 
    # so we use MemFileStorageProvider to buffer changes in memory
    # and create new meta provider with memory storage
    # this is less secure way, but quicker
    mem_storage_provider = MemFileStorageProvider()

    mem_storage_provider.upload_file_str(volume_full_path, volume_storage_provider.get_file_str_content(volume_full_path))
    meta_provider = FileMetaProvider(
        file_name="meta.json",
        path=volume_path,
        store_versions=False,
        storage_provider=mem_storage_provider,
    )


    # Create model manager that works with target meta provider
    model_manager = DbxModelManager(
        spark=session.spark, meta_provider=meta_provider
    )

    return model_manager, volume_path, session, volume_storage_provider, mem_storage_provider

def test_dbx_model_manager(
        spark = None, 
        dbutils = None, 
        env = "tst", 
        do_cleanup = True):

    model_manager, volume_path, _, volume_storage_provider, mem_storage_provider = get_manager(
        spark=spark,
        dbutils=dbutils,
        env=env,
        params_path="_model_definition",
        model_path="_model_definition"
    )

    was_exception = False
    try:
        
        # All the magic here - Provision model, 
        # by default only PENDING are processed, 
        # but can be changed to process other statuses as well
        model_manager.provision_model()

        # If somehow meta is inconsistent (e.g. table was created 
        # but code interrupted before meta was updated),
        # below method can sync meta with its real state
        #model_manager.sync_meta()

    except Exception as e:
        print(f"Test failed with error: {e}")
        was_exception = True
        raise
    finally:
        # After provisioning, sync meta file from memory storage to volume storage
        meta_file = Path(volume_path).joinpath("meta.json").as_posix()
        volume_storage_provider.upload_file_str(meta_file,mem_storage_provider.get_file_str_content(meta_file))

        # Do cleanup if required
        if do_cleanup or was_exception:
            run_cleanup(spark, dbutils, env)