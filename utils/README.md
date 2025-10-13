# Overview
**poweretl** is library to support database, data-warehouse, data-lakehouse python projects. *poweretl-utils* is independent package for not only etl tasks.

Read more about library on git-hub: [poweretl](https://github.com/plazacraft/poweretl)

# Features
| Class                                       | Features
| ------------------------------------------- | --------------------------------------------------------------- |
| **poweretl.utils.file.MultiFileReader**     | Read multiple files from folder in lexicographical order, using regex for filter. |
| **poweretl.utils.file.FileMerger**            | Reads multiple supported files (e.g. json, yaml) and merge the output based on provided order. As a result it produce dict collection. This is very help for multi file configuration, where there is general config and dedicated for target environments.   |
| **poweretl.utils.file.FileCommandSplitter**   | Split content of many files to File-Version-Command tuples. It is useful to run commands based on version.  |
| **poweretl.utils.text.TokensReplacer**        | Dedicated to replace token values in text based on the input dictionary.          |




