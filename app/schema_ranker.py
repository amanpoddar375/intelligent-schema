from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .config import SchemaConfig


@dataclass
class EmbeddingStore:
    table_embeddings: Dict[str, List[float]]
    column_embeddings: Dict[Tuple[str, str], List[float]]
    vectorizer: TfidfVectorizer


class SchemaRanker:
    """Ranks tables and columns by semantic relevance."""

    def __init__(self, cfg: SchemaConfig, store: EmbeddingStore | None = None):
        self._cfg = cfg
        self._store = store

    def rank_tables(self, query: str, schema_snapshot: Dict, top_n: int | None = None) -> List[str]:
        top_n = top_n or self._cfg.ranker_top_n
        scored = self._score_tables(query, schema_snapshot)
        scored.sort(key=lambda x: x[1], reverse=True)
        return [table for table, _ in scored[:top_n]]

    def _score_tables(self, query: str, schema_snapshot: Dict) -> List[Tuple[str, float]]:
        if self._store:
            return self._score_with_embeddings(query, schema_snapshot)
        return self._score_with_tfidf(query, schema_snapshot)

    def _score_with_embeddings(self, query: str, schema_snapshot: Dict) -> List[Tuple[str, float]]:
        vectorizer = self._store.vectorizer
        query_vec = vectorizer.transform([query])
        results: List[Tuple[str, float]] = []
        for table, meta in schema_snapshot.get("tables", {}).items():
            description = meta.get("description") or ""
            columns = meta.get("columns", {})
            corpus = [description] + [col.get("description") or col_name for col_name, col in columns.items()]
            table_vec = vectorizer.transform([" ".join(corpus)])
            score = float(cosine_similarity(query_vec, table_vec)[0][0])
            column_boost = self._column_overlap_boost(query, columns.keys())
            results.append((table, score + column_boost))
        return results

    def _score_with_tfidf(self, query: str, schema_snapshot: Dict) -> List[Tuple[str, float]]:
        documents: List[str] = []
        keys: List[str] = []
        for table, meta in schema_snapshot.get("tables", {}).items():
            doc_parts = [table, meta.get("description", "")]
            for col_name, col_meta in meta.get("columns", {}).items():
                doc_parts.append(col_name)
                if desc := col_meta.get("description"):
                    doc_parts.append(desc)
            documents.append(" ".join(doc_parts))
            keys.append(table)
        if not documents:
            return []
        vectorizer = TfidfVectorizer(stop_words="english")
        matrix = vectorizer.fit_transform(documents)
        query_vec = vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, matrix)[0]
        return list(zip(keys, similarities))

    def _column_overlap_boost(self, query: str, columns: Iterable[str]) -> float:
        if not query:
            return 0.0
        score = 0.0
        lower_query = query.lower()
        for column in columns:
            if column.lower() in lower_query:
                score += 0.1
        return min(score, 0.5)


def rank_tables(query: str, schema_snapshot: Dict, top_n: int = 10) -> List[str]:
    ranker = SchemaRanker(SchemaConfig())
    return ranker.rank_tables(query, schema_snapshot, top_n=top_n)


__all__ = ["SchemaRanker", "EmbeddingStore", "rank_tables"]
