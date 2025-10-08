from poweretl.common.model.config import *
import poweretl.common.utils.tests as common_tests
import pytest
import os
import re


def test_get_model():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    config = JsonConfigProvider(
        file_paths=[
            MultiFileReader.FileEntry(f'{module_dir}/_data/', r'\.json$')
        ],
        encoding='utf-8'
    )

    model = config.get_model()
