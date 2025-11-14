from abc import ABC, abstractmethod

from poweretl.defs.meta import Meta
from poweretl.defs.model import BaseItem, Model


class IMetaProvider(ABC):
    """Keeps Model metadata and it's provisioning state."""

    def __init__(self):
        pass

    @abstractmethod
    def self_update(self):
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
    def push_meta_item_changes(self, item: BaseItem):
        pass

    @abstractmethod
    def get_meta(self, statuses: set[str] = None, table_id: str = None) -> Meta:
        """Returns Model together with its metadata."""


