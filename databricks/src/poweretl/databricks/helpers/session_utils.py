# pylint: disable=C0411, C0103, W0611
try:

    from databricks.connect import DatabricksSession  # noqa: F401
    spark = None
    dbutils = None
    display = None
except ImportError:
    pass
