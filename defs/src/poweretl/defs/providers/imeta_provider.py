from abc import ABC, abstractmethod

from poweretl.defs.meta import Meta
from poweretl.defs.model import Model


class IMetaProvider(ABC):
    """Keeps Model metadata and it's provisioning state."""

    def __init__(self):
        pass

    @abstractmethod
    def update_self_model(self):
        pass

    @abstractmethod
    def push_model_changes(self, model: Model):
        """Push model information to meta.

        Args:
            model (Model): Model changes to be updated.
        """

    @abstractmethod
    def push_meta_changes(self, meta: Meta):
        """Push new meta information about target model

        Args:
            meta (Meta): Meta information that needs to be updated
        """

    @abstractmethod
    def get_meta(self, table_id: str = None) -> Meta:
        """Returns Model together with its metadata."""
