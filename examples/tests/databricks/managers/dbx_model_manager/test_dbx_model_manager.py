# pylint: disable=protected-access, W0621, R0914

from pathlib import Path

from poweretl.common import FileMetaProvider
from poweretl.utils import MemFileStorageProvider
from poweretl.utils.tests import deep_compare_true

from poweretl.databricks.helpers import get_or_connect
from poweretl.databricks.managers import DbxModelManager


def run_cleanup(spark):
    pass

def test_dbx_model_manager(spark = None, dbutils = None, run_cleanup = True):

    # if run from databricks, spark and dbutils will be provided and it takes priority over config file
    # if these are not provided, we connect using config file
    config_path = ""
    session = get_or_connect(spark=spark, dbutils=dbutils, config_path=config_path)

    if run_cleanup:
        run_cleanup(spark)