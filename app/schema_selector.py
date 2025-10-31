from __future__ import annotations

import json
from typing import Dict, Iterable

from .config import SchemaConfig


def select_schema_slice(snapshot: Dict, table_ids: Iterable[str], cfg: SchemaConfig) -> Dict:
    slice_tables = {}
    total_bytes = 0
    fk_set = []

    tables = snapshot.get("tables", {})
    for table_id in table_ids:
        meta = tables.get(table_id)
        if not meta:
            continue
        serialized = json.dumps(meta)
        total_bytes += len(serialized.encode("utf-8"))
        if total_bytes > cfg.max_schema_slice_bytes:
            break
        slice_tables[table_id] = meta

    for fk in snapshot.get("foreign_keys", []):
        fk_tables = {fk.get("table"), fk.get("foreign_table")}
        if fk_tables <= set(slice_tables.keys()):
            fk_set.append([
                fk.get("table"),
                _extract_fk_column(fk.get("definition", ""), 1),
                fk.get("foreign_table"),
                _extract_fk_column(fk.get("definition", ""), 2),
            ])

    return {
        "tables": slice_tables,
        "foreign_keys": fk_set,
    }


def _extract_fk_column(definition: str, index: int) -> str:
    # Fallback parser for FK definitions like: FOREIGN KEY (col) REFERENCES schema.table(col)
    try:
        parts = definition.split("(")
        col = parts[index].split(")")[0].strip()
        return col
    except Exception:
        return ""


__all__ = ["select_schema_slice"]
