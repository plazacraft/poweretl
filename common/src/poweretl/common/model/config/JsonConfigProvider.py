import poweretl.defs.model.config as cnf
from poweretl.common.utils.file import *
import json
from dacite import from_dict
from dataclasses import asdict


class JsonConfigProvider(cnf.IConfigProvider):
    """ JSON configuration provider.
    Attributes:
        file_paths (list[(str, str)]): Absolute paths to folder to scan for files and regex for names.
        encoding (str, optional): Encoding to use for reading files. Defaults to 'utf-8'.
    """
    def __init__(self, file_paths: list[FileEntry], encoding:str='utf-8'):
        super().__init__()
        self._MultiFileReader = MultiFileReader(file_paths=file_paths, encoding=encoding)

    def get_model(self) -> cnf.Model:
        data = None
        files, contents = self._MultiFileReader.get_files_with_content()
        json_merger = JsonFileMerger()
        file_contents = [contents[file] for file in files]
        data = json_merger.merge(file_contents)
        if (data):
            return from_dict(data_class=cnf.Model, data=data)
        else:
            return {}

    def to_json(self, model: cnf.Model, dump_params = {}) -> str:
        return json.dumps(asdict(model), **dump_params)
