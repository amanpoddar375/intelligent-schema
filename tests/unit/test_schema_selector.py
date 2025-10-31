from __future__ import annotations

from app.config import SchemaConfig
from app.schema_selector import select_schema_slice


def test_schema_selector_limits_bytes() -> None:
    snapshot = {
        "tables": {
            f"public.table{i}": {
                "columns": {"col": {}}
            }
            for i in range(3)
        },
        "foreign_keys": [
            {
                "table": "public.table0",
                "foreign_table": "public.table1",
                "definition": "FOREIGN KEY (col) REFERENCES public.table1(col)"
            }
        ]
    }
    cfg = SchemaConfig(max_schema_slice_bytes=200)
    slice_snapshot = select_schema_slice(snapshot, ["public.table0", "public.table1", "public.table2"], cfg)
    assert "public.table0" in slice_snapshot["tables"]
    assert len(slice_snapshot["tables"]) >= 1
