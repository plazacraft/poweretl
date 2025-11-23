# pylint: disable=R0914


from poweretl.defs import IMetaProvider, Meta, Model

from poweretl.common.helpers import MetaModelUpdater


class BaseMetaProvider(IMetaProvider):
    """Keeps Model metadata and it's provisioning state in file."""

    def __init__(self):
        # instantiate helper that encapsulates _v... logic
        self._meta_updater = MetaModelUpdater()

    def _get_updated_meta(self, model: Model, meta: Meta) -> Meta:
        """Return an updated Meta object based on provided model.

        This makes a deepcopy of meta, updates its fields from model while
        respecting excluded fields, and updates nested collections.
        """
        return self._meta_updater.get_updated_meta(model, meta)

    def _apply_status_filter(self, meta: Meta, statuses: set[str]) -> Meta:
        return self._meta_updater.apply_status_filter(meta, statuses)

    def _find_by_object_id(self, meta: Meta, object_id: str):
        """Find a meta item by object id or by passing a
        dataclass item with .meta.object_id.

        Returns the found dataclass instance or None.
        """
        return self._meta_updater.find_by_object_id(meta, object_id)
