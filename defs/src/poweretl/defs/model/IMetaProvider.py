from poweretl.defs.model.config import *
from abc import ABC, abstractmethod

class IMetaProvider(ABC):
    """ Provides configuration of model.
    """
    def __init__(self, model: Model):
        self._model = model

    @abstractmethod
    def update_model(self):
        pass

