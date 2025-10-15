from abc import ABC, abstractmethod

from poweretl.defs import Model


class IModelProvider(ABC):
    """Provides configuration of model."""

    def __init__(self):
        pass

    @abstractmethod
    def get_model(self) -> Model:
        pass
