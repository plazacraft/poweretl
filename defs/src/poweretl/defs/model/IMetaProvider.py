from abc import ABC, abstractmethod

from poweretl.defs.model.config import *


class IMetaProvider(ABC):
    """Provides configuration of model."""

    def __init__(self, model: Model):
        self._model = model

    @abstractmethod
    def update_model(self):
        pass
