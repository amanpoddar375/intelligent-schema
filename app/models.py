from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    user_id: Optional[str] = None
    refresh_schema: bool = False


class QueryResponse(BaseModel):
    answer: str
    sql: str
    rows: List[Dict[str, Any]]
    metadata: Dict[str, Any]


__all__ = ["QueryRequest", "QueryResponse"]
