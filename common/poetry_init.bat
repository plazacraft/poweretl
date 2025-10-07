poetry install --with dev
poetry run pip install -e "../defs/" --force-reinstall --no-deps
poetry lock
poetry install

