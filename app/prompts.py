from __future__ import annotations

import json
import pathlib
from typing import Any, Dict

from jsonschema import Draft7Validator

from .config import PromptsConfig


class PromptResources:
    def __init__(self, cfg: PromptsConfig):
        self.examples = _load_json(cfg.examples_path)
        self.reasoner_schema = _load_json(cfg.reasoner_schema)
        self.synthesizer_schema = _load_json(cfg.synthesizer_schema)
        self.reasoner_validator = Draft7Validator(self.reasoner_schema)
        self.synthesizer_validator = Draft7Validator(self.synthesizer_schema)
        self.base_dir = pathlib.Path(cfg.examples_path).parent


def _load_json(path: str) -> Dict[str, Any]:
    path_obj = pathlib.Path(path)
    if not path_obj.is_absolute():
        path_obj = pathlib.Path.cwd() / path_obj
    with path_obj.open("r", encoding="utf-8") as fh:
        return json.load(fh)


__all__ = ["PromptResources"]
