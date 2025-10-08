import poweretl.defs.model.config as cnf
from poweretl.common.utils.file import *
import json
from deepmerge import Merger
from dacite import from_dict


class JsonConfigProvider(cnf.IConfigProvider):
    """ JSON configuration provider.
    Attributes:
        file_paths (list[(str, str)]): Absolute paths to folder to scan for files and regex for names.
        encoding (str, optional): Encoding to use for reading files. Defaults to 'utf-8'.
    """
    def __init__(self, file_paths: MultiFileReader.FileEntry, encoding:str='utf-8'):
        super().__init__()
        self.MultiFileReader = MultiFileReader(file_paths=file_paths, encoding=encoding)

    # default strategy of always_merger
    _merger = Merger(
        [
            (dict, "merge"), 
            (list, "append"), 
            (set, "union")
        ],
        ["override"],
        ["override"]
    )



    def get_model(self) -> cnf.Model:
        data = None
        files, contents = self.MultiFileReader.get_files_with_content()
        for file in files:
            json_data = None
            content = contents[file]
            if content:
                json_data = json.loads(content)

            if json_data:
                if data is None:
                    data = json_data
                else:
                    self._merger.merge(data, json_data)

        return from_dict(data_class=cnf.Model, data=data)


