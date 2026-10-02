"""
Tests for deserialize_sqale and build_database.

Run with:  pytest tests/test_import.py -v
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from sqale import build_database, deserialize_sqale


def test_basic_deserialization(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir))

    assert [r["schema_id"] for r in results] == ["schema_001", "schema_002"]
    for r in results:
        assert r["error"] is None, f"Unexpected error for {r['schema_id']}: {r['error']}"
        assert Path(r["db_path"]).exists(), f".db file not created: {r['db_path']}"


def test_split_selection(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir), split="test")
    assert [r["schema_id"] for r in results] == ["schema_101"]


def test_rows_per_table(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir))

    by_id = {r["schema_id"]: r for r in results}
    assert by_id["schema_001"]["rows_per_table"] == {"users": 2, "orders": 2}
    # tables created by the DDL are listed even when they hold no rows
    assert by_id["schema_002"]["tables"] == ["things", "empty_one"]
    assert by_id["schema_002"]["rows_per_table"]["empty_one"] == 0


def test_db_is_queryable(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir))

    schema_001 = next(r for r in results if r["schema_id"] == "schema_001")
    conn = sqlite3.connect(schema_001["db_path"])
    rows = conn.execute("SELECT name FROM users ORDER BY id").fetchall()
    conn.close()

    assert rows == [("Alice",), ("Bob",)]


def test_db_opens_read_only_without_side_files(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir))

    db_path = results[0]["db_path"]
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 2
    conn.close()
    assert sorted(p.name for p in output_dir.iterdir()) == ["schema_001.db", "schema_002.db"]


def test_limit_parameter(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir), limit=1)
    assert len(results) == 1, "limit=1 should produce only one result"


def test_limit_zero(schemas_repo, output_dir):
    assert deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir), limit=0) == []


def test_schema_ids_filter(schemas_repo, output_dir):
    results = deserialize_sqale(
        file_path=str(schemas_repo), output_dir=str(output_dir), schema_ids=["schema_002"]
    )
    assert [r["schema_id"] for r in results] == ["schema_002"]
    assert [p.name for p in output_dir.iterdir()] == ["schema_002.db"]


def test_unknown_schema_id_reported(schemas_repo, output_dir):
    results = deserialize_sqale(
        file_path=str(schemas_repo), output_dir=str(output_dir), schema_ids=["schema_001", "schema_101"]
    )
    by_id = {r["schema_id"]: r for r in results}
    assert by_id["schema_001"]["error"] is None
    assert by_id["schema_101"]["db_path"] is None
    assert "not found in split 'train'" in by_id["schema_101"]["error"]


def test_existing_db_replaced(schemas_repo, output_dir):
    for _ in range(2):
        results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir), limit=1)
    assert results[0]["error"] is None
    assert results[0]["rows_per_table"] == {"users": 2, "orders": 2}


def test_values_converted_for_sqlite(schemas_repo, output_dir):
    results = deserialize_sqale(file_path=str(schemas_repo), output_dir=str(output_dir), split="test")

    conn = sqlite3.connect(results[0]["db_path"])
    rows = conn.execute('SELECT meta, big, "odd ""name""" FROM t').fetchall()
    conn.close()
    # nested values are stored as JSON, out-of-range integers are clamped,
    # and the row with a duplicate primary key is skipped
    assert rows == [(json.dumps({"a": [1, 2]}), 9223372036854775807, "x")]


def test_output_dir_created(tmp_path, schemas_repo):
    new_dir = tmp_path / "brand_new_dir"
    assert not new_dir.exists()

    deserialize_sqale(file_path=str(schemas_repo), output_dir=str(new_dir))

    assert new_dir.exists()


def test_missing_local_path(output_dir):
    with pytest.raises(FileNotFoundError):
        deserialize_sqale(file_path="./does/not/exist", output_dir=str(output_dir))


def test_hub_repo_reads_only_the_split(monkeypatch, schemas_repo, output_dir):
    """A repo ID is read shard by shard through huggingface_hub (mocked here)."""
    import huggingface_hub

    downloaded = []
    monkeypatch.setattr(huggingface_hub.HfApi, "list_repo_files", lambda self, repo, repo_type=None: [
        ".gitattributes", "README.md", "figures/pipeline.png",
        "data/test-00000-of-00001.parquet", "data/train-00000-of-00001.parquet",
    ])

    def fake_download(repo, filename, repo_type=None):
        downloaded.append(filename)
        return str(schemas_repo / filename)

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_download)
    results = deserialize_sqale(file_path="someone/sqale2_schemas", output_dir=str(output_dir), split="test")

    assert [r["schema_id"] for r in results] == ["schema_101"]
    assert downloaded == ["data/test-00000-of-00001.parquet"]


def test_single_file_ignores_split(schemas_repo, output_dir):
    test_file = schemas_repo / "data" / "test-00000-of-00001.parquet"
    results = deserialize_sqale(file_path=str(test_file), output_dir=str(output_dir), split="train")
    assert [r["schema_id"] for r in results] == ["schema_101"]


def test_build_database_in_memory(schemas_repo):
    row = pd.read_parquet(schemas_repo / "data" / "train-00000-of-00001.parquet").iloc[0].to_dict()
    conn = build_database(row)
    assert conn.execute("SELECT SUM(amount) FROM orders").fetchone()[0] == pytest.approx(149.49)
    conn.close()


def test_build_database_to_file(schemas_repo, tmp_path):
    row = pd.read_parquet(schemas_repo / "data" / "train-00000-of-00001.parquet").iloc[0].to_dict()
    path = tmp_path / "one.sqlite"
    build_database(row, path).close()
    with pytest.raises(FileExistsError):
        build_database(row, path)
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 2
    conn.close()


# ---------------------------------------------------------------------------
# Older single-table layout (trl-lab/SQaLe_2)
# ---------------------------------------------------------------------------

def test_legacy_layout(sample_parquet, output_dir):
    results = deserialize_sqale(file_path=str(sample_parquet), output_dir=str(output_dir))

    assert len(results) == 2
    schema_001 = next(r for r in results if r["schema_id"] == "schema_001")
    assert schema_001["error"] is None
    assert schema_001["rows_per_table"] == {"users": 2, "orders": 2}


def test_legacy_deduplication(tmp_path, output_dir):
    """Rows with the same schema_id should be deduplicated."""
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
