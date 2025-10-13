from abc import ABC, abstractmethod

from .model import Model


class IConfigProvider(ABC):
    """Provides configuration of model."""

    def __init__(self):
        pass

    @abstractmethod
    def get_model(self) -> Model:
        pass
