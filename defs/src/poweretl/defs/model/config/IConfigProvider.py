from .Model import *
from abc import ABC, abstractmethod

class IConfigProvider(ABC):
    """ Provides configuration of model.
    """
    def __init__(self):
        pass

    @abstractmethod
    def get_model(self) -> Model:
        pass

