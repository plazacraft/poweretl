from poweretl.common import BaseModelManager
from poweretl.defs import IMetaProvider
from pyspark.sql import SparkSession


class DbxModelManager(BaseModelManager):

    def __init__(self, spark: SparkSession, meta_provider: IMetaProvider):
        super().__init__(meta_provider = meta_provider)
        self._spark = spark

    def _execute_command(self, command: str):
        self._spark.sql(command)

