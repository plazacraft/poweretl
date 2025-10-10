from pathlib import Path
from poweretl.defs.model.config import Model
from poweretl.common.utils.file import MultiFileReader, FileEntry, FileMerger
from poweretl.common.utils.text import TokensReplacer
from poweretl.defs.model.config import IConfigProvider
import json
from dacite import from_dict
from dataclasses import asdict



class FileConfigProvider(IConfigProvider):
    """_summary_

    Args:
        IConfigProvider (_type_): _description_
    """
    

    def __init__(self, 
                 config_paths: list[FileEntry], 
                 param_paths: list[FileEntry] = None,
                 encoding:str='utf-8',
                 tokens_replacer: TokensReplacer = TokensReplacer(start="/*<", end=">*/", escape="^")
                 ):
    
        self._config_reader = MultiFileReader(file_paths=config_paths, encoding=encoding)
        self._param_reader = MultiFileReader(file_paths=param_paths, encoding=encoding)
        self._tokens_replacer = tokens_replacer
        self._file_merger = FileMerger()
            

    def get_model(self) -> Model:
        data = None

        params = self._param_reader.get_files_with_content()
        configs = self._config_reader.get_files_with_content()
        params_data = None

        if (not configs):
            return {}
        
        if params:
            params_data = self._file_merger.merge(params)

        if (params_data):
            config_contents = [(config, self._tokens_replacer.replace(tokens=params_data, text=content)) for config, content in configs]
        else:
            config_contents = configs

        data = self._file_merger.merge(config_contents)
        
        if (data):
            return from_dict(data_class=Model, data=data)
        else:
            return {}

    def to_json(self, model: Model, dump_params = {}) -> str:
        return json.dumps(asdict(model), **dump_params)
