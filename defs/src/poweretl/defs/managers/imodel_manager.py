from abc import ABC, abstractmethod


class IModelManager(ABC):
    """Provides configuration of model."""

    def __init__(self):
        pass

    @abstractmethod
    def provision_model(self, table_id: str = None):
        pass
