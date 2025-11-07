from abc import ABC, abstractmethod
from poweretl.defs.meta import Status

class IModelManager(ABC):
    """Provides configuration of model."""

    def __init__(self):
        pass

    @abstractmethod
    def provision_model(self, statuses: set[Status] = {Status.PENDING.value}, table_id: str = None):
        pass
