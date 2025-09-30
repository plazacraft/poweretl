from .Model import *
from abc import ABC, abstractmethod

class ConfigProvider(ABC):
    """ Provides configuration of model.
    """
    def __init__(self):
        pass

    @abstractmethod
    def get_config(self) -> Model:
        pass

