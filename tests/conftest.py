"""Shared fixtures for the sqale test suite."""

import json
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest


SAMPLE_DDL = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER
);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount REAL
)
"""

SAMPLE_CONTENT = {
    "users": [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
    ],
    "orders": [
        {"id": 1, "user_id": 1, "amount": 99.99},
        {"id": 2, "user_id": 2, "amount": 49.50},
    ],
}


@pytest.fixture()
def sample_parquet(tmp_path: Path) -> Path:
    """Write a minimal SQaLe-shaped parquet file and return its path."""
    df = pd.DataFrame(
        [
            {
                "schema id": "schema_001",
                "Full schema": SAMPLE_DDL,
                "Schema content": json.dumps(SAMPLE_CONTENT),
            },
            {
                "schema id": "schema_002",
                "Full schema": "CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT)",
                "Schema content": json.dumps(
                    {"things": [{"id": 1, "label": "foo"}, {"id": 2, "label": "bar"}]}
                ),
            },
        ]
    )
    parquet_file = tmp_path / "sample.parquet"
    df.to_parquet(str(parquet_file), index=False)
    return parquet_file


@pytest.fixture()
def output_dir(tmp_path: Path) -> Path:
    """Return a fresh temporary output directory."""
    d = tmp_path / "dbs"
    d.mkdir()
    return d
