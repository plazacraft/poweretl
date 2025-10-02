from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, Any

@dataclass
class TestConfig:
    name: str
    expected: Any