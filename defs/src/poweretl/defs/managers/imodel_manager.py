from abc import ABC, abstractmethod

from poweretl.defs.meta import Meta


class IModelManager(ABC):
    """Provides configuration of model."""

    def __init__(self):
        pass

    @abstractmethod
    def provision_model():
        pass