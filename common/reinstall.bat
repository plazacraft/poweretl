cd ..\defs
call .venv\Scripts\activate.bat
poetry build

cd ..\common
call .venv\Scripts\activate.bat
poetry run pip install "../defs/dist/poweretl_defs-0.1.0-py3-none-any.whl" --force-reinstall --no-deps
poetry run pip install "./dist/poweretl_common-0.1.0-py3-none-any.whl" --force-reinstall --no-deps
poetry lock
poetry install 

