import poweretl.defs.model.config as cnf
from poweretl.common.model.config._MultiFileReader import _MultiFileReader
import deepmerge
import json
from dacite import from_dict


class JsonConfigProvider(cnf.IConfigProvider):
    """ JSON configuration provider.
    Attributes:
        file_path (str): Path to the JSON configuration file.
        encoding (str, optional): Encoding of the JSON file. Default is 'utf-8'.
    """
    def __init__(self, regex: str, file_paths: list[str], encoding:str='utf-8'):
        super().__init__()
        self._MultiFileReader = _MultiFileReader(regex=regex, file_paths=file_paths, encoding=encoding)

    def get_model(self) -> cnf.Model:
        data = None
        files, contents = self._MultiFileReader.get_files_with_content()
        for file in files:
            json_data = None
            content = contents[file]
            if content:
                json_data = json.loads(content)

            if json_data:
                if data is None:
                    data = json_data
                else:
                    data = deepmerge.merge(data, json_data)

        return from_dict(data_class=cnf.Model, data=data)


