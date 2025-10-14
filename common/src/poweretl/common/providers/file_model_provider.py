import json
from dataclasses import asdict

from dacite import from_dict
from poweretl.defs.model import Model
from poweretl.defs.providers import IModelProvider
from poweretl.utils.file import FileEntry, FileMerger, MultiFileReader
from poweretl.utils.text import TokensReplacer


class FileModelProvider(IModelProvider):
    """Provides model definition from files

    Attributes:
        config_paths (list[FileEntry]): Files with model definitions
        param_paths (list[FileEntry], optional): Files with parameters
        encoding (str, optional): Encoding of files. Defaults to "utf-8".
        tokens_replacer (TokensReplacer, optional): Tokens replacer used
            to replace tokens with provided parameters.
    """

    def __init__(
        self,
        config_paths: list[FileEntry],
        param_paths: list[FileEntry] = None,
        encoding: str = "utf-8",
        tokens_replacer: TokensReplacer = TokensReplacer(
            re_start=r"(/\*<)|(<)", re_end=r"(>\*/)|(>)", re_escape=r"\^"
        ),
    ):
        self._config_reader = MultiFileReader(
            file_paths=config_paths, encoding=encoding
        )
        self._param_reader = MultiFileReader(file_paths=param_paths, encoding=encoding)
        self._tokens_replacer = tokens_replacer
        self._file_merger = FileMerger()

    def get_model(self) -> Model:
        data = None

        params = self._param_reader.get_files_with_content()
        configs = self._config_reader.get_files_with_content()
        params_data = None

        if not configs:
            return {}

        if params:
            params_data = self._file_merger.merge(params)

        if params_data:
            config_contents = [
                (
                    config,
                    self._tokens_replacer.replace(tokens=params_data, text=content),
                )
                for config, content in configs
            ]
        else:
            config_contents = configs

        data = self._file_merger.merge(config_contents)

        if data:
            return from_dict(data_class=Model, data=data)

        return {}

    def to_json(self, model: Model, dump_params={}) -> str:
        return json.dumps(asdict(model), **dump_params)
