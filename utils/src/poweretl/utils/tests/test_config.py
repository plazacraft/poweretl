from dataclasses import dataclass
from typing import Any


@dataclass
class TestConfig:
    name: str
    expected: Any
