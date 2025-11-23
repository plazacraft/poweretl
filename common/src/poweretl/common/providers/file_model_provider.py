import json
from dataclasses import asdict

from dacite import from_dict
from poweretl.defs import IModelProvider, Model
from poweretl.utils import FileEntry, FileMerger, MultiFileReader, TokensReplacer


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

        self.params = None
        params_content = self._param_reader.get_files_with_content()
        if params_content:
            self.params = self._file_merger.merge(params_content)

        self.config = {}
        configs_content = self._config_reader.get_files_with_content()

        if self.params:
            configs_content_final = [
                (
                    config,
                    self._tokens_replacer.replace(tokens=self.params, text=content),
                )
                for config, content in configs_content
            ]
        else:
            configs_content_final = configs_content

        if configs_content_final:
            self.config = self._file_merger.merge(configs_content_final)

    def get_model(self) -> Model:

        if self.config:
            return from_dict(data_class=Model, data=self.config)

        return {}

    def to_json(self, model: Model, dump_params={}) -> str:
        return json.dumps(asdict(model), **dump_params)
