from .test_dbx_model_manager import *
from poweretl.databricks.helpers.session_utils import *  # if run not from databricks, define spark and dbutils as null # noqa: F403, F401, E501 # pylint: disable=W0401, W0614

test_dbx_model_manager(spark, dbutils, run_cleanup=False)
