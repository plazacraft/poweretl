cd ..\defs
poetry build
cd ..\common
poetry build
cd ..\databricks
poetry build
cd ..\examples
poetry env remove
del poetry.lock
poetry install --with dev