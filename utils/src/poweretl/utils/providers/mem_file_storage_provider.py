# pylint: disable=R0914
from pathlib import Path

from .ifile_storage_provider import IFileStorageWriter


class MemFileStorageProvider(IFileStorageWriter):

    def __init__(self):
        self._mem_storage = {}

    def get_first_file_or_folder(
        self, path: str, ascending: bool = True
    ) -> tuple[str, bool]:
        # _mem_storage keys are full file paths (strings). We need to find
        # immediate children under `path` and return the first one according
        # to sort order. If the child is a directory (there exist keys under
        # that child), return is_dir=True.
        if not self._mem_storage:
            return None

        prefix = Path(path).as_posix().rstrip("/") + "/"
        children = {}
        for full in self._mem_storage.keys():  # pylint: disable=C0201
            full_path = Path(full).as_posix()
            if not full_path.startswith(prefix):
                continue
            remainder = full_path[len(prefix) :]  # noqa: E203
            if not remainder:
                # exact file equals prefix (unlikely since prefix ends with /)
                child_name = Path(full_path).name
            else:
                child_name = remainder.split("/")[0]

            children.setdefault(child_name, []).append(full_path)

        if not children:
            return None

        # separate directories and files so we prefer directories over files
        dirs = []
        files = []
        for name, paths in children.items():
            chosen_path = Path(prefix).joinpath(name).as_posix()
            is_dir = any(p.startswith(chosen_path.rstrip("/") + "/") for p in paths)
            if is_dir:
                dirs.append(name)
            else:
                files.append(name)

        def pick_from(lst):
            if not lst:
                return None
            sorted_list = sorted(lst, key=lambda n: n.lower(), reverse=not ascending)
            return sorted_list[0]

        chosen = pick_from(dirs) or pick_from(files)
        chosen_path = Path(prefix).joinpath(chosen).as_posix()
        is_dir = any(
            p.startswith(chosen_path.rstrip("/") + "/") for p in children[chosen]
        )
        return (chosen_path, is_dir)

    def get_folders_list(self, path: str, recursive: bool = False) -> list[str]:
        prefix = Path(path).as_posix().rstrip("/") + "/"
        folders = set()
        for full in self._mem_storage.keys():  # pylint: disable=C0201
            full_path = Path(full).as_posix()
            if not full_path.startswith(prefix):
                continue
            remainder = full_path[len(prefix) :]  # noqa: E203
            parts = remainder.split("/")
            # build folder paths
            if len(parts) >= 2:
                # there is at least one folder under prefix
                if recursive:
                    # include all intermediate folder levels
                    for i in range(1, len(parts)):
                        folders.add(
                            Path(prefix).joinpath("/".join(parts[:i])).as_posix()
                        )
                else:
                    # only immediate child folder
                    folders.add(Path(prefix).joinpath(parts[0]).as_posix())

        return sorted(folders)

    def get_files_list(self, path: str, recursive: bool = False) -> list[str]:
        root = Path(path)
        files = []
        if recursive:
            files = root.rglob("*")
        else:
            files = root.iterdir()

        all_files = [file.resolve() for file in files if file.is_file()]
        return all_files

    def get_file_str_content(self, full_path: str) -> str:
        # Return the content stored in memory for the exact path
        return self._mem_storage.get(Path(full_path).as_posix())

    def upload_file_str(self, full_path: str, content: str):
        self._mem_storage[full_path] = content
