import csv
import os
from dataclasses import asdict

from poweretl.utils import FileCommandSplitter, FileEntry, FileMerger, MultiFileReader
from poweretl.utils.text import TokensReplacer


def test_get_commands():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = f"{module_dir}/_commands"

    # use default configurations
    command_splitter = FileCommandSplitter()
    replacer = TokensReplacer()
    merger = FileMerger()

    # read command files in Lexicographical order
    commands_reader = MultiFileReader([FileEntry(f"{data_dir}", r"\.sql$")])
    commands_files = commands_reader.get_files_with_content()

    # read params from yaml files, global and prod (omit test)
    param_reader = MultiFileReader(
        [
            FileEntry(f"{data_dir}", r"global\.yaml$"),
            FileEntry(f"{data_dir}", r"prod\.yaml$"),
        ]
    )
    param_files = param_reader.get_files_with_content()
    params = merger.merge(param_files)

    # update content based on parameters using TokensReplacer
    commands_contents = [
        (command_file, replacer.replace(tokens=params, text=content))
        for command_file, content in commands_files
    ]

    # get commands collection
    commands = command_splitter.get_commands(commands_contents)

    # extract commands to csv
    with open(
        f"{module_dir}/_results/commands.csv",
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(f, fieldnames=commands[0].__dataclass_fields__.keys())
        writer.writeheader()
        for command in commands:
            writer.writerow(asdict(command))
