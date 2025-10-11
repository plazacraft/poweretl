poetry lock
poetry install --with dev
poetry run pip install -e "../defs/" --force-reinstall --no-deps
poetry run pip install -e "../utils/" --force-reinstall --no-deps
poetry lock
poetry install

