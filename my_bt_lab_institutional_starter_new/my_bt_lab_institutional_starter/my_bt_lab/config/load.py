from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml


def load_yaml_config(path: str | Path) -> Tuple[Dict[str, Any], Path]:
    p = Path(path).resolve()
    with open(p, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg, p
