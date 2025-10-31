from __future__ import annotations

import json
from typing import Any, Dict, List

from .config import LLMConfig
from .llm_client import LLMClient
from .logging_utils import get_logger
from .prompts import PromptResources

logger = get_logger(__name__)


class ResponseSynthesizer:
    def __init__(self, cfg: LLMConfig, llm: LLMClient, prompts: PromptResources):
        self._cfg = cfg
        self._llm = llm
        self._prompts = prompts

    async def synthesize(self, query: str, sql: str, rows: List[Dict[str, Any]], metadata: Dict[str, Any]) -> str:
        messages = self._build_messages(query, sql, rows, metadata)
        payload = {"messages": messages}
        logger.info("response_synthesizer_request", rows=len(rows))
        result = await self._llm.complete_json(payload)
        errors = list(self._prompts.synthesizer_validator.iter_errors(result))
        if errors:
            raise ValueError("Synthesizer returned invalid JSON: " + "; ".join(e.message for e in errors))
        return result.get("response", "")

    def _build_messages(self, query: str, sql: str, rows: List[Dict[str, Any]], metadata: Dict[str, Any]):
        system_msg = {
            "role": "system",
            "content": "You produce human friendly summaries using only provided rows. Output JSON only."
        }
        example_msgs = []
        for example in self._prompts.examples.get("synthesizer_examples", []):
            example_msgs.append({"role": "user", "content": json.dumps({
                "query": example["user_query"],
                "sql": example["sql"],
                "rows": example["rows"],
                "metadata": example["metadata"]
            })})
            example_msgs.append({"role": "assistant", "content": json.dumps({
                "response": example["expected_output"],
                "highlights": []
            })})
        user_msg = {
            "role": "user",
            "content": json.dumps({
                "query": query,
                "sql": sql,
                "rows": rows,
                "metadata": metadata
            })
        }
        return [system_msg, *example_msgs, user_msg]


async def synthesize_response(query: str, sql: str, rows: List[Dict[str, Any]], metadata: Dict[str, Any]) -> str:
    raise NotImplementedError("Instantiate ResponseSynthesizer with dependencies")


__all__ = ["ResponseSynthesizer", "synthesize_response"]
