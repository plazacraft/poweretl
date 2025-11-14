from poweretl.common import BaseModelManager
from poweretl.defs import IMetaProvider
from pyspark.sql import SparkSession  # pylint: disable=C0411
from poweretl.defs.meta import Status
from poweretl.defs.model import Table

class DbxModelManager(BaseModelManager):

    def __init__(self, spark: SparkSession, meta_provider: IMetaProvider):
        super().__init__(meta_provider=meta_provider)
        self._spark = spark

    def _execute_command(self, command: str):
        self._spark.sql(command)

    def get_table_model_from_source(table_name) -> Table:
        pass


    def sync_meta(self, statuses: set[str] = {Status.PENDING.value}, table_id: str = None):
        #meta_to_sync = self._meta_provider.get_meta(statuses=statuses, table_id=table_id)

        pass