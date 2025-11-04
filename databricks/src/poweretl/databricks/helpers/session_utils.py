from pyspark.sql import SparkSession


try:
    from databricks.connect import DatabricksSession
    dbutils = None
    display = None
except ImportError:
    pass

