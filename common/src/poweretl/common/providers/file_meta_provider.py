from poweretl.defs import IMetaProvider, Meta, Model


class FileMetaProvider(IMetaProvider):
    """Keeps Model metadata and it's provisioning state."""

    def __init__(self):
        pass

    def update_self_to_version(self, version: str):
        pass

    def push_model_changes(self, model: Model):
        pass

    def push_meta_changes(self, meta: Meta):
        pass

    def get_model_meta(self) -> Meta:
        pass
