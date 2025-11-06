# Overview


| Name               | Description |  Example |
| ------------------ | ----------- |  ------- |
| [Multi File Reader](src/poweretl/utils/file/multi_file_reader.py) | Read multiple files from folder in lexicographical order, using regex for filter. | [Commands Splitter](../examples/tests/utils/file/commands_splitter/) <br /> [Files Merger](../examples/tests/utils/file/files_merger/) |
| [Tokens Replacer](src/poweretl/utils/text/tokens_replacer.py) | Dedicated to replace token values in text based on the input dictionary. | [Commands Splitter](../examples/tests/utils/file/commands_splitter/) <br />[Files Merger](../examples/tests/utils/file/files_merger/) |
| [Commands Splitter](src/poweretl/utils/file/file_command_splitter.py) | Split content of many files to File-Version-Command tuples. It is useful to run commands based on version. | [Commands Splitter](../examples/tests/utils/file/commands_splitter/) |
| [Files Merger](src/poweretl/utils/file/file_merger.py) | Merge json and yaml files together (based on order) to provide one collection | [Files Merger](../examples/tests/utils/file/files_merger/) |
| [Os File Storage Provider](src/poweretl/utils/providers/os_file_storage_provider.py) | A provider that implements operations on files that are storage on Operation System ||
| [Mem File Storage Provider](src/poweretl/utils/providers/mem_file_storage_provider.py) | A provider that implements operations on files that are storage in Memory, use mostly for tests ||


