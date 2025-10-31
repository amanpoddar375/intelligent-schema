from __future__ import annotations

import abc
import json
from typing import Any, Dict

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import LLMConfig


class LLMError(RuntimeError):
    pass


class LLMClient(abc.ABC):
    @abc.abstractmethod
    async def complete_json(self, prompt: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class EchoLLMClient(LLMClient):
    async def complete_json(self, prompt: Dict[str, Any]) -> Dict[str, Any]:
        messages = prompt.get("messages", [])
        if not messages:
            return {}
        content = messages[-1].get("content", "{}")
        try:
            payload = json.loads(content)
        except json.JSONDecodeError:
            return {}
        if "schema_slice" in payload:
            tables = list(payload.get("schema_slice", {}).get("tables", {}).keys())
            schema_context = {}
            for table_name, meta in payload.get("schema_slice", {}).get("tables", {}).items():
                columns = list(meta.get("columns", {}).keys())[:5]
                schema_context[table_name] = {"columns": columns}
            return {
                "query_intent": payload.get("query", ""),
                "relevant_tables": tables,
                "schema_context": schema_context,
                "foreign_keys_map": payload.get("schema_slice", {}).get("foreign_keys", []),
                "performance_hints": [],
            }
        if "rows" in payload:
            rows = payload.get("rows", [])
            response = f\"Returned {len(rows)} rows.\"
            return {
                "response": response,
                "highlights": [],
            }
        return payload


class OpenAIClient(LLMClient):
    def __init__(self, cfg: LLMConfig, api_key: str):
        self._cfg = cfg
        self._api_key = api_key
        self._client = httpx.AsyncClient(timeout=30)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def complete_json(self, prompt: Dict[str, Any]) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
        }
        payload = {
            "model": self._cfg.model,
            "response_format": {"type": "json_object"},
            "temperature": self._cfg.temperature,
            "max_tokens": self._cfg.max_tokens,
            **prompt,
        }
        async for attempt in AsyncRetrying(
            reraise=True,
            stop=stop_after_attempt(self._cfg.reasoner_retry_config.attempts),
            wait=wait_exponential(multiplier=1, min=1, max=5),
            retry=retry_if_exception_type(LLMError),
        ):
            with attempt:
                resp = await self._client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                )
                if resp.status_code >= 400:
                    raise LLMError(f"LLM HTTP {resp.status_code}: {resp.text}")
                data = resp.json()
                try:
                    content = data["choices"][0]["message"]["content"]
                except (KeyError, IndexError) as exc:
                    raise LLMError("Unexpected LLM response") from exc
                try:
                    return json.loads(content)
                except json.JSONDecodeError as exc:
                    raise LLMError("LLM did not return valid JSON") from exc


def build_llm_client(cfg: LLMConfig, api_key: str) -> LLMClient:
    if cfg.provider.lower() == "openai":
        if not api_key:
            return EchoLLMClient()
        return OpenAIClient(cfg, api_key)
    raise ValueError(f"Unsupported LLM provider: {cfg.provider}")


__all__ = ["LLMClient", "OpenAIClient", "EchoLLMClient", "build_llm_client", "LLMError"]
