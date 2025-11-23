# flake8: noqa: F405, E402, F403
# pylint: disable=C0413, W0401, E0401, E0602

# import os
# import sys

# sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from test_dbx_model_manager import *

from poweretl.databricks.helpers.session_utils import *  # if run not from databricks, define spark and dbutils as null # noqa: F403, F401, E501 # pylint: disable=W0401, W0614

test_dbx_model_manager(spark, dbutils, env="tst", do_cleanup=False)

run_cleanup(spark, dbutils, env="tst")
