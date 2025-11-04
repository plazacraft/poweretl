from pyspark.sql import SparkSession


# if not in databricks environment, define None for dbutils and display for globals
try:
    spark = SparkSession.builder.getOrCreate()
except:
    dbutils = None
    display = None
