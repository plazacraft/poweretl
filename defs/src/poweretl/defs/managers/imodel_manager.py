from abc import ABC, abstractmethod
from poweretl.defs.meta import Status
from poweretl.defs.model import Table

class IModelManager(ABC):
    """Provides configuration of model."""

    def __init__(self):
        pass

    @abstractmethod
    def provision_model(self, statuses: set[str] = {Status.PENDING.value}, table_id: str = None):
        pass

    @abstractmethod
    def get_table_model_from_source(table_name) -> Table:
        pass    

    @abstractmethod
    def sync_meta(self, statuses: set[str] = {Status.PENDING.value}, table_id: str = None):
        pass
