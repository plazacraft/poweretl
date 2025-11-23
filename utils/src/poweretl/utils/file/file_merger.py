from deepmerge import Merger, always_merger

from .file_serializer import FileSerializer


class FileMerger:
    """Merges files and returns dictionary object.


    Attributes:
        file_serializer (FileSerializer): Serializes files to dict,
            only files supported by target file_serializer can be used
        merger (Merger): Merger strategy,
            as default always_merger is used
    """

    def __init__(
        self, file_serializer=FileSerializer(), merger: Merger = always_merger
    ):
        self._file_serializer = file_serializer
        self._merger = merger
        # default strategy of always_merger
        # self._merger = Merger(
        #     [
        #         (dict, "merge"),
        #         (list, "append"),
        #         (set, "union")
        #     ],
        #     ["override"],
        #     ["override"]
        # )

    def _to_dict(self, file: str, content) -> dict:
        return self._file_serializer.to_dict(file, content)

    def merge(self, files: list[tuple[str, str]]) -> dict:
        """Mere files.

        Args:
            files (list[tuple[str, str]]): List of files and their contents.

        Returns:
            dict: Dictionary of objects after merging.
        """
        data = None
        for file, content in files:
            file_data = None
            if content:
                file_data = self._to_dict(file, content)

            if file_data:
                if data is None:
                    data = file_data
                else:
                    self._merger.merge(data, file_data)
        return data
