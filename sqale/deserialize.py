"""
Deserialize the SQaLe dataset (cwolff/data_work_in_progress) into SQLite .db files
and expose question-level benchmark data.

Each unique schema in the dataset is materialized as a .db file populated
with the synthetic data stored in the 'Schema content' column.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def deserialize_sqale(
    file_path: str = "cwolff/data_work_in_progress",
    output_dir: str = "deserialized_dbs",
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Load the SQaLe dataset, deduplicate by schema_id, and materialize each
    unique schema as a populated SQLite .db file.

    Parameters
    ----------
    file_path:
        Path to a local parquet/arrow file, a directory of such files, or a
        HuggingFace dataset repo ID (default: 'cwolff/data_work_in_progress').
    output_dir:
        Directory where the .db files will be written (created if missing).
    limit:
        Maximum number of unique schemas to process.  None means process all.

    Returns
    -------
    list of dicts, each containing:
        schema_id      – original schema id from the dataset
        db_path        – absolute path to the created .db file
        tables         – list of table names found in the DDL
        rows_per_table – dict mapping table_name → number of rows inserted
        error          – None on success, error message string on failure
    """
    dataset = _load_dataset(file_path)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    row_iter = _iter_rows(dataset)

    results = []
    seen_schema_ids: set[str] = set()
    schemas: list = []

    gather_limit = limit
    with tqdm(total=gather_limit, desc="Gathering schemas") as pbar:
        for row in row_iter:
            schema_id = str(row.get("schema id") or "unknown")
            if schema_id not in seen_schema_ids:
                seen_schema_ids.add(schema_id)
                schemas.append(row)
                pbar.update(1)
                if gather_limit is not None and len(schemas) >= gather_limit:
                    break

    for row in tqdm(schemas, total=len(schemas), desc="Schemas"):
        schema_id = str(row.get("schema id") or "unknown")
        full_schema = row.get("Full schema") or ""
        schema_content_raw = row.get("Schema content") or "{}"

        schema_content = _parse_json_field(schema_content_raw, default={})

        safe_id = re.sub(r"[^\w\-]", "_", schema_id)
        db_path = out / f"{safe_id}.db"

        try:
            rows_per_table = _materialize_db(db_path, full_schema, schema_content)
            error = None
        except Exception as exc:
            rows_per_table = {}
            error = str(exc)

        results.append({
            "schema_id": schema_id,
            "db_path": str(db_path.resolve()),
            "tables": list(rows_per_table.keys()),
            "rows_per_table": rows_per_table,
            "error": error,
        })

    return results


def load_questions(
    file_path: str = "cwolff/data_work_in_progress",
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Load question-level benchmark data from the SQaLe dataset.

    Each row in the dataset represents one question linked to a database schema.
    Unlike :func:`deserialize_sqale`, this function does *not* deduplicate by
    schema_id — it returns one entry per question.

    Parameters
    ----------
    file_path:
        Path to a local parquet/arrow file, a directory of such files, or a
        HuggingFace dataset repo ID (default: 'cwolff/data_work_in_progress').
    limit:
        Maximum number of questions to return.  None means return all.

    Returns
    -------
    list of dicts, each containing:
        question_id      – unique question identifier (e.g. 'q_0000000')
        schema_id        – associated schema id
        difficulty       – difficulty label (e.g. 'simple', 'moderate', 'challenging')
        questions        – dict with question formulations keyed by style
                           (e.g. 'verbose (original)', 'short_high_level', 'casual')
        sql              – gold SQL statement
        relevant_tables  – list of relevant table names
        execution_result – expected query result (list of rows)
    """
    dataset = _load_dataset(file_path)

    results = []
    for row in tqdm(_iter_rows(dataset), desc="Loading questions"):
        results.append({
            "question_id": str(row.get("question id") or ""),
            "schema_id": str(row.get("schema id") or ""),
            "difficulty": str(row.get("difficulty") or ""),
            "questions": _parse_json_field(row.get("questions"), default={}),
            "sql": str(row.get("sql statament") or ""),
            "relevant_tables": _parse_json_field(row.get("relevant tables"), default=[]),
            "execution_result": _parse_json_field(row.get("execution_result"), default=[]),
        })
        if limit is not None and len(results) >= limit:
            break

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _iter_rows(dataset):
    """Normalise dataset to a plain iterable of dict-like rows."""
    if isinstance(dataset, pd.DataFrame):
        return (row for _, row in dataset.iterrows())
    return iter(dataset)


def _parse_json_field(raw, *, default):
    """Parse a JSON string field; return *default* on failure."""
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, type(default)) or default is None:
                return parsed
            return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return default


def _load_dataset(file_path: str):
    """Load the dataset from a local file/directory or a HuggingFace repo ID.

    For local files returns a pandas DataFrame.
    For HuggingFace repo IDs returns a streaming IterableDataset so nothing
    is written to disk.
    """
    p = Path(file_path)
    if p.exists():
        if p.is_dir():
            frames = []
            for ext in ("*.parquet", "*.arrow"):
                for f in sorted(p.glob(ext)):
                    frames.append(_read_single_file(f))
            if not frames:
                raise FileNotFoundError(f"No parquet/arrow files found in {p}")
            return pd.concat(frames, ignore_index=True)
        return _read_single_file(p)

    try:
        from datasets import load_dataset  # type: ignore
        return load_dataset(file_path, split="train", streaming=True)
    except Exception as exc:
        raise ValueError(
            f"Could not load dataset from '{file_path}': {exc}"
        ) from exc


def _read_single_file(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(str(path))
    if path.suffix == ".arrow":
        from datasets import Dataset  # type: ignore
        return Dataset.from_file(str(path)).to_pandas()
    raise ValueError(f"Unsupported file type: {path.suffix}")


def _split_ddl(ddl: str) -> list[str]:
    return [s.strip() for s in ddl.split(";") if s.strip()]


def _materialize_db(
    db_path: Path,
    ddl: str,
    schema_content: dict[str, list[dict]],
) -> dict[str, int]:
    """
    Create a SQLite database at *db_path*, execute the DDL, then insert all
    rows from *schema_content*.

    Returns a mapping of table_name → number of rows inserted.
    """
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA journal_mode = WAL")

        for stmt in _split_ddl(ddl):
            try:
                conn.execute(stmt)
            except sqlite3.Error:
                pass

        rows_per_table: dict[str, int] = {}
        for table, table_rows in schema_content.items():
            if not isinstance(table_rows, list):
                try:
                    table_rows = list(table_rows)
                except TypeError:
                    rows_per_table[table] = 0
                    continue
            table_rows = [
                dict(r) if not isinstance(r, dict) else r
                for r in table_rows
            ]
            if len(table_rows) == 0:
                rows_per_table[table] = 0
                continue

            cols = list(table_rows[0].keys())
            col_list = ", ".join(f'"{c}"' for c in cols)
            placeholders = ", ".join("?" * len(cols))
            insert_sql = (
                f'INSERT OR IGNORE INTO "{table}" ({col_list}) VALUES ({placeholders})'
            )

            inserted = 0
            for row_dict in table_rows:
                values = _coerce_row(row_dict, cols)
                try:
                    conn.execute(insert_sql, values)
                    inserted += 1
                except sqlite3.Error:
                    pass

            rows_per_table[table] = inserted

        conn.commit()
    finally:
        conn.close()

    return rows_per_table


def _coerce_row(row_dict: dict, cols: list[str]) -> list:
    """Clamp numeric values to SQLite-safe ranges and return an ordered list."""
    values = []
    for c in cols:
        val = row_dict.get(c)
        if isinstance(val, int):
            val = max(-9223372036854775808, min(9223372036854775807, val))
        elif isinstance(val, float):
            val = max(-1.7976931348623157e+308, min(1.7976931348623157e+308, val))
        values.append(val)
    return values


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Deserialize the SQaLe dataset into SQLite .db files."
    )
    p.add_argument(
        "--input",
        default="cwolff/data_work_in_progress",
        help="Local parquet/arrow file, directory, or HuggingFace repo ID (e.g. cwolff/data_work_in_progress).",
    )
    p.add_argument(
        "--output",
        default="deserialized_dbs",
        help="Output directory for .db files (default: deserialized_dbs).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of unique schemas to process.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    results = deserialize_sqale(
        file_path=args.input,
        output_dir=args.output,
        limit=args.limit,
    )
    failures = [r for r in results if r["error"]]
    successes = len(results) - len(failures)
    total_rows = sum(sum(r["rows_per_table"].values()) for r in results)
    print(
        f"Done: {successes}/{len(results)} succeeded, {total_rows:,} rows total."
    )
    for r in failures:
        print(f"  FAIL {r['schema_id']}: {r['error']}")
