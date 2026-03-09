#!/usr/bin/env python3
"""
Deserialize the SQaLe dataset (trl-lab/SQaLe_2) into SQLite .db files.

Each unique schema in the dataset is materialized as a .db file populated
with the synthetic data stored in the 'Schema content' column.

Usage:
    python deserialize_sqale.py --output ./dbs --limit 100
    python deserialize_sqale.py --output ./dbs
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm  # pip install tqdm


# ---------------------------------------------------------------------------
# Core deserialization
# ---------------------------------------------------------------------------

def deserialize_sqale(
    file_path: str,
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
        HuggingFace dataset repo ID (e.g. 'cwolff/whatever_100k').
    output_dir:
        Directory where the .db files will be written (created if missing).
    limit:
        Maximum number of unique schemas to process.  None means process all.

    Returns
    -------
    list of dicts, each containing:
        schema_id     – original schema id from the dataset
        db_path       – absolute path to the created .db file
        tables        – list of table names found in the DDL
        rows_per_table – dict mapping table_name → number of rows inserted
    """
    df = _load_dataset(file_path)

    # Keep only the first occurrence of each schema_id to avoid duplicate work
    df = df.drop_duplicates(subset=["schema id"])
    if limit is not None:
        df = df.iloc[:limit]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    results = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Schemas"):
        schema_id = str(row.get("schema id") or "unknown")
        full_schema = row.get("Full schema") or ""
        schema_content_raw = row.get("Schema content") or "{}"

        schema_content = _parse_schema_content(schema_content_raw)

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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_schema_content(raw) -> dict[str, list[dict]]:
    """Parse the Schema content field into a dict[table → list[row]]."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def _load_dataset(file_path: str) -> pd.DataFrame:
    """Load the dataset from a local file/directory or a HuggingFace repo ID."""
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

    # Fall back to HuggingFace
    try:
        from datasets import load_dataset  # type: ignore
        ds = load_dataset(file_path, split="train")
        return ds.to_pandas()
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
    """Split a DDL string into individual statements."""
    return [s.strip() for s in ddl.split(";") if s.strip()]


def _materialize_db(
    db_path: Path,
    ddl: str,
    schema_content: dict[str, list[dict]],
) -> dict[str, int]:
    """
    Create a SQLite database at *db_path*, execute the DDL to build the
    schema, then insert all rows from *schema_content*.

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
                pass  # Ignore unsupported syntax / duplicate table errors

        rows_per_table: dict[str, int] = {}
        for table, table_rows in schema_content.items():
            # Normalise: pandas may return rows as a list of dicts, a list of
            # numpy objects, or something else after a parquet round-trip.
            if not isinstance(table_rows, list):
                try:
                    table_rows = list(table_rows)
                except TypeError:
                    rows_per_table[table] = 0
                    continue
            # Each row should be a plain dict; coerce if necessary.
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
        file_path="trl-lab/SQaLe_2",
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


if __name__ == "__main__":
    main()
