from poweretl.common.model.config import *
import poweretl.common.tests as common_tests
import pytest
import os
import re


def test_get_model():
    module_dir = os.path.dirname(os.path.abspath(__file__))

    config = JsonConfigProvider(
        regex=r'\.config\.json$',
        file_paths=[
            f'{module_dir}/_data/01',
            f'{module_dir}/_data/02'
        ],
        encoding='utf-8'
    )

    model = config.get_model()
