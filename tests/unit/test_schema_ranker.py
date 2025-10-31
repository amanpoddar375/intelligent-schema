from __future__ import annotations

from app.config import SchemaConfig
from app.schema_ranker import SchemaRanker


def test_schema_ranker_prefers_matching_table() -> None:
    snapshot = {
        "tables": {
            "public.claims": {
                "description": "Insurance claims filed by customers",
                "columns": {"claim_id": {}, "customer_id": {}, "status": {}}
            },
            "public.shipments": {
                "description": "Shipment records and tracking",
                "columns": {"shipment_id": {}, "carrier": {}}
            },
        }
    }
    ranker = SchemaRanker(SchemaConfig())
    ranked = ranker.rank_tables("claims for customers", snapshot, top_n=1)
    assert ranked[0] == "public.claims"
