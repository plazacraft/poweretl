from pathlib import Path
from poweretl.defs.model.config import Model
from poweretl.common.utils.file import MultiFileReader, FileEntry, JsonFileMerger, YamlFileMerger
from poweretl.common.utils.text import TokensReplacer
from poweretl.defs.model.config import IConfigProvider
import json
from dacite import from_dict
from dataclasses import asdict



class FileConfigProvider(IConfigProvider):

    SUPPORTED_EXTENSIONS = ['.json', '.yaml', '.yml']
    
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
            

    def _validate_extensions(self, paths: list[Path]) -> str:
        extensions = {p.suffix for p in paths if p.is_file()}
        if len(extensions) > 1:
            raise ValueError(f"Inconsistent file extensions for provided files: {extensions}")
        if not extensions:
            raise ValueError("No valid file extensions found.")

        extension = extensions.pop()

        if extension not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file extension: {extension}")

        return extension

    def _get_merger(self, extension: str):
        if extension == '.json':
            return JsonFileMerger()
        elif extension in ['.yaml', '.yml']:
            return YamlFileMerger()
        return None

    def get_model(self) -> Model:
        data = None

        params, p_contents = self._param_reader.get_files_with_content()
        configs, c_contents = self._config_reader.get_files_with_content()
        params_data = None

        if (not configs):
            return {}
        
        if params:
            param_extension = self._validate_extensions(params)
            param_merger = self._get_merger(param_extension)
            param_contents = [p_contents[param] for param in params]
            params_data = param_merger.merge(param_contents)

        if (params_data):
            config_contents = [self._tokens_replacer.replace(tokens=params_data, text=c_contents[config]) for config in configs]
        else:
            config_contents = [c_contents[config] for config in configs]

        config_extension = self._validate_extensions(configs)
        config_merger = self._get_merger(config_extension)
        data = config_merger.merge(config_contents)
        
        if (data):
            return from_dict(data_class=Model, data=data)
        else:
            return {}

    def to_json(self, model: Model, dump_params = {}) -> str:
        return json.dumps(asdict(model), **dump_params)
