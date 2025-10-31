from __future__ import annotations

import json
from typing import Dict

from .config import LLMConfig
from .llm_client import LLMClient
from .logging_utils import get_logger
from .prompts import PromptResources

logger = get_logger(__name__)


class LLMReasoner:
    def __init__(self, cfg: LLMConfig, llm: LLMClient, prompts: PromptResources):
        self._cfg = cfg
        self._llm = llm
        self._prompts = prompts

    async def reason_schema_with_llm(self, query: str, schema_slice: Dict) -> Dict:
        messages = self._build_messages(query, schema_slice)
        payload = {"messages": messages}
        logger.info("llm_reasoner_request", query=query, tables=len(schema_slice.get("tables", {})))
        result = await self._llm.complete_json(payload)
        errors = list(self._prompts.reasoner_validator.iter_errors(result))
        if errors:
            details = "; ".join(err.message for err in errors)
            logger.warning("llm_reasoner_invalid_json", details=details)
            raise ValueError(f"Reasoner returned invalid schema: {details}")
        self._enforce_schema_bounds(result, schema_slice)
        return result

    def _build_messages(self, query: str, schema_slice: Dict):
        system_msg = {
            "role": "system",
            "content": "You are a schema reasoning engine. Respond with strict JSON only."
        }
        examples = self._prompts.examples.get("reasoner_examples", [])
        example_msgs = []
        for example in examples:
            example_msgs.append({"role": "user", "content": json.dumps({
                "query": example["user_query"],
                "schema_slice": example["schema_slice"]
            })})
            example_msgs.append({"role": "assistant", "content": json.dumps(example["expected_output"])} )
        user_msg = {
            "role": "user",
            "content": json.dumps({"query": query, "schema_slice": schema_slice})
        }
        return [system_msg, *example_msgs, user_msg]

    def _enforce_schema_bounds(self, result: Dict, schema_slice: Dict) -> None:
        allowed_tables = set(schema_slice.get("tables", {}).keys())
        for table in result.get("relevant_tables", []):
            if table not in allowed_tables:
                raise ValueError(f"LLM referenced unknown table {table}")
        for table, payload in result.get("schema_context", {}).items():
            if table not in allowed_tables:
                raise ValueError(f"LLM referenced unknown context table {table}")
            allowed_columns = set(schema_slice["tables"][table]["columns"].keys())
            for column in payload.get("columns", []):
                if column not in allowed_columns:
                    raise ValueError(f"LLM referenced unknown column {table}.{column}")


async def reason_schema_with_llm(query: str, schema_slice: Dict) -> Dict:
    raise NotImplementedError("Instantiate LLMReasoner with dependencies")


__all__ = ["LLMReasoner", "reason_schema_with_llm"]
