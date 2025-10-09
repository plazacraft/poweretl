from pathlib import Path
from poweretl.defs.model.config import Model
from poweretl.common.utils.file import MultiFileReader, FileEntry, FileMerger
from poweretl.common.utils.text import TokensReplacer
from poweretl.defs.model.config import IConfigProvider
import json
from dacite import from_dict
from dataclasses import asdict



class FileConfigProvider(IConfigProvider):

    
    """File Configuration provider.
    """
    def __init__(self, 
                 config_paths: list[FileEntry], 
                 param_paths: list[FileEntry] = None,
                 encoding:str='utf-8',
                 token_replacer: TokensReplacer = None
                 ):
    
        self._config_reader = MultiFileReader(file_paths=config_paths, encoding=encoding)
        self._param_reader = MultiFileReader(file_paths=param_paths, encoding=encoding)
        if (not token_replacer):
            self._tokens_replacer = TokensReplacer(start="<", end=">", escape="^")
        else:
            self._tokens_replacer = token_replacer
        self._file_merger = FileMerger()
            

    def get_model(self) -> Model:
        data = None

        params, p_contents = self._param_reader.get_files_with_content()
        configs, c_contents = self._config_reader.get_files_with_content()
        params_data = None

        if (not configs):
            return {}
        
        if params:
            params_data = self._file_merger.merge(params, p_contents)

        if (params_data):
            config_contents = {config: self._tokens_replacer.replace(tokens=params_data, text=c_contents[config]) for config in configs}
        else:
            config_contents = c_contents

        data = self._file_merger.merge(configs, config_contents)
        
        if (data):
            return from_dict(data_class=Model, data=data)
        else:
            return {}

    def to_json(self, model: Model, dump_params = {}) -> str:
        return json.dumps(asdict(model), **dump_params)
