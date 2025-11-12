import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from test_dbx_model_manager import *
from poweretl.databricks.helpers.session_utils import *  # if run not from databricks, define spark and dbutils as null # noqa: F403, F401, E501 # pylint: disable=W0401, W0614


# TODO: delete twice will try do delete
# TODO: undelete is not working 

#test_dbx_model_manager(spark, dbutils, env="tst", do_cleanup=False)

run_cleanup(spark, dbutils, env="tst")
