

from poweretl.defs import IModelManager, Meta, IMetaProvider


class BaseModelManager(IModelManager):

    def __init__(self, meta_provider: IMetaProvider):
        self._meta_provider = meta_provider

    def provision_model(self):
        meta = self._meta_provider.get_meta()
        pass