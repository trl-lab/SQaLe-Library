"""
Tests for the sqale Python API (import usage).

Run with:  pytest tests/test_import.py -v
"""

import sqlite3
from pathlib import Path

import pytest

from sqale import deserialize_sqale


def test_basic_deserialization(sample_parquet, output_dir):
    results = deserialize_sqale(
        file_path=str(sample_parquet),
        output_dir=str(output_dir),
    )

    assert len(results) == 2, "Should produce one result per unique schema"
    for r in results:
        assert r["error"] is None, f"Unexpected error for {r['schema_id']}: {r['error']}"
        assert Path(r["db_path"]).exists(), f".db file not created: {r['db_path']}"


def test_rows_inserted(sample_parquet, output_dir):
    results = deserialize_sqale(
        file_path=str(sample_parquet),
        output_dir=str(output_dir),
    )

    schema_001 = next(r for r in results if r["schema_id"] == "schema_001")
    assert schema_001["rows_per_table"].get("users") == 2
    assert schema_001["rows_per_table"].get("orders") == 2


def test_db_is_queryable(sample_parquet, output_dir):
    results = deserialize_sqale(
        file_path=str(sample_parquet),
        output_dir=str(output_dir),
    )

    schema_001 = next(r for r in results if r["schema_id"] == "schema_001")
    conn = sqlite3.connect(schema_001["db_path"])
    rows = conn.execute("SELECT name FROM users ORDER BY id").fetchall()
    conn.close()

    assert rows == [("Alice",), ("Bob",)]


def test_limit_parameter(sample_parquet, output_dir):
    results = deserialize_sqale(
        file_path=str(sample_parquet),
        output_dir=str(output_dir),
        limit=1,
    )

    assert len(results) == 1, "limit=1 should produce only one result"


def test_output_dir_created(tmp_path, sample_parquet):
    new_dir = tmp_path / "brand_new_dir"
    assert not new_dir.exists()

    deserialize_sqale(file_path=str(sample_parquet), output_dir=str(new_dir))

    assert new_dir.exists()


def test_deduplication(tmp_path, output_dir):
    """Rows with the same schema_id should be deduplicated."""
    import json
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "schema id": "dup_schema",
                "Full schema": "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                "Schema content": json.dumps({"t": [{"id": 1}]}),
            },
            {
                "schema id": "dup_schema",  # duplicate
                "Full schema": "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                "Schema content": json.dumps({"t": [{"id": 2}]}),
            },
        ]
    )
    pq = tmp_path / "dup.parquet"
    df.to_parquet(str(pq), index=False)

    results = deserialize_sqale(file_path=str(pq), output_dir=str(output_dir))

    assert len(results) == 1, "Duplicate schema_ids should be deduplicated"
