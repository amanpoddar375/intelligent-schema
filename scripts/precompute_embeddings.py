#!/usr/bin/env python3
"""Precompute TF-IDF embeddings for schema tables and columns."""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer


def build_corpus(snapshot: dict) -> tuple[list[str], list[str]]:
    documents: list[str] = []
    keys: list[str] = []
    for table, meta in snapshot.get("tables", {}).items():
        doc = [table, meta.get("description", "")]
        for column, col_meta in meta.get("columns", {}).items():
            doc.append(column)
            if desc := col_meta.get("description"):
                doc.append(desc)
        documents.append(" ".join(doc))
        keys.append(table)
    return documents, keys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    documents, keys = build_corpus(snapshot)
    vectorizer = TfidfVectorizer(stop_words="english")
    matrix = vectorizer.fit_transform(documents)
    embeddings = {
        key: matrix[idx].toarray().tolist()[0]
        for idx, key in enumerate(keys)
    }
    payload = {
        "vectorizer": vectorizer,
        "embeddings": embeddings,
    }
    args.output.write_bytes(pickle.dumps(payload))


if __name__ == "__main__":
    main()
