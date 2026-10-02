"""
Load the SQaLe text-to-SQL dataset and turn its databases into SQLite files.

SQaLe is published as two Hugging Face datasets that join on ``schema_id``:

* ``trl-lab/sqale2_schemas`` – one row per database: the DDL (``full_schema``)
  and the generated rows of its tables (``schema_content``).
* ``trl-lab/sqale2_queries`` – one row per question: the gold SQL, the
  difficulty, eight phrasings of the question and the gold query's result.

Both have a schema-disjoint ``train`` / ``test`` split.  Files in the older
single-table layout (``schema id``, ``Full schema``, ``Schema content``, ...)
are still read.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable, Iterator, Optional, Union

from tqdm import tqdm


SCHEMAS_REPO = "trl-lab/sqale2_schemas"
QUERIES_REPO = "trl-lab/sqale2_queries"

STYLES = (
    "verbose",
    "evidence_supported",
    "structured",
    "requirements_list",
    "short_ambiguous",
    "short_high_level",
    "casual",
    "spelling_grammar_mistakes",
)

# Placeholders left by the style-variation step: "...", "<string>", "**Moderate**".
_PLACEHOLDER = re.compile(
    r"[\s.…]*|<[^>]*>|\**\s*(simple|moderate|hard)\s*\**", re.IGNORECASE
)

# Column names of the single-table releases (trl-lab/SQaLe_2,
# cwolff/data_work_in_progress), mapped to the current ones.
_LEGACY_COLUMNS = {
    "schema id": "schema_id",
    "Full schema": "full_schema",
    "Schema content": "schema_content",
    "question id": "question_id",
    "sql statament": "sql",
    "relevant tables": "relevant_tables",
    "number of relevant tables": "number_of_relevant_tables",
}
_LEGACY_STYLES = {"verbose (original)": "verbose"}

_INT64_MIN, _INT64_MAX = -9223372036854775808, 9223372036854775807
_FLOAT_MAX = 1.7976931348623157e308


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def deserialize_sqale(
    file_path: str = SCHEMAS_REPO,
    output_dir: str = "deserialized_dbs",
    limit: Optional[int] = None,
    split: str = "train",
    schema_ids: Optional[Iterable[str]] = None,
) -> list[dict]:
    """
    Materialize SQaLe databases as populated SQLite ``.db`` files.

    Parameters
    ----------
    file_path:
        HuggingFace dataset repo ID (default: 'trl-lab/sqale2_schemas'), a
        local parquet/arrow file, or a directory of such files.
    output_dir:
        Directory where the .db files will be written (created if missing).
        An existing ``<schema_id>.db`` there is replaced.
    limit:
        Maximum number of databases to write.  None means all.
    split:
        Dataset split to read, 'train' or 'test'.  Ignored for a single
        local file.
    schema_ids:
        Only write these databases.  Ids that are not in the split come back
        as results with an error.

    Returns
    -------
    list of dicts, each containing:
        schema_id      – schema id from the dataset
        db_path        – absolute path to the created .db file
        tables         – tables created in the database
        rows_per_table – dict mapping table_name → number of rows it holds
        error          – None on success, error message string on failure
    """
    wanted = None if schema_ids is None else {str(s) for s in schema_ids}
    target = limit
    if wanted is not None:
        target = len(wanted) if limit is None else min(limit, len(wanted))
    if target == 0:
        return []

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    seen: set[str] = set()
    with tqdm(total=target, desc="Databases") as pbar:
        for row in _iter_rows(file_path, split):
            row = _normalise(row)
            schema_id = str(row.get("schema_id") or "unknown")
            if schema_id in seen or (wanted is not None and schema_id not in wanted):
                continue
            seen.add(schema_id)
            results.append(_write_database(row, schema_id, out))
            pbar.update(1)
            if target is not None and len(results) >= target:
                break

    if wanted is not None and len(results) < target:
        for schema_id in sorted(wanted - seen):
            results.append({
                "schema_id": schema_id,
                "db_path": None,
                "tables": [],
                "rows_per_table": {},
                "error": f"schema_id not found in split '{split}' of {file_path}",
            })
    return results


def load_questions(
    file_path: str = QUERIES_REPO,
    limit: Optional[int] = None,
    split: str = "train",
    difficulty: Optional[Union[str, Iterable[str]]] = None,
    drop_placeholders: bool = True,
) -> list[dict]:
    """
    Load question-level data from the SQaLe dataset, one entry per question.

    Parameters
    ----------
    file_path:
        HuggingFace dataset repo ID (default: 'trl-lab/sqale2_queries'), a
        local parquet/arrow file, or a directory of such files.
    limit:
        Maximum number of questions to return.  None means return all.
    split:
        Dataset split to read, 'train' or 'test'.  Ignored for a single
        local file.
    difficulty:
        Keep only questions of this difficulty ('simple', 'moderate',
        'hard'), or of any difficulty in a list of them.
    drop_placeholders:
        Replace placeholder phrasings such as '...' with None (see
        :func:`is_usable_question`).

    Returns
    -------
    list of dicts, each containing:
        question_id               – unique question identifier
        schema_id                 – the database the question is asked over
        difficulty                – 'simple', 'moderate' or 'hard'
        questions                 – dict of phrasings keyed by style (see STYLES);
                                    a value is None where no usable phrasing exists
        sql                       – gold SQL statement
        relevant_tables           – tables of the subschema the question was
                                    generated from; the gold SQL uses a subset
        number_of_relevant_tables – length of relevant_tables
        execution_result          – first rows of the gold query's result
    """
    if limit == 0:
        return []
    if difficulty is None:
        levels = None
    elif isinstance(difficulty, str):
        levels = {difficulty}
    else:
        levels = set(difficulty)

    results: list[dict] = []
    for row in tqdm(_iter_rows(file_path, split), desc="Loading questions"):
        row = _normalise(row)
        level = str(row.get("difficulty") or "")
        if levels is not None and level not in levels:
            continue
        questions = _parse_questions(row.get("questions"))
        if drop_placeholders:
            questions = {k: (v if is_usable_question(v) else None) for k, v in questions.items()}
        relevant = _parse_json_field(row.get("relevant_tables"), default=[])
        results.append({
            "question_id": str(row.get("question_id") or ""),
            "schema_id": str(row.get("schema_id") or ""),
            "difficulty": level,
            "questions": questions,
            "sql": str(row.get("sql") or ""),
            "relevant_tables": relevant,
            "number_of_relevant_tables": int(row.get("number_of_relevant_tables") or len(relevant)),
            "execution_result": _parse_json_field(row.get("execution_result"), default=[]),
        })
        if limit is not None and len(results) >= limit:
            break
    return results


def build_database(schema: dict, path: Union[str, Path] = ":memory:") -> sqlite3.Connection:
    """
    Create a SQLite database from one row of ``trl-lab/sqale2_schemas``.

    Parameters
    ----------
    schema:
        A dataset row with ``full_schema`` and ``schema_content`` (the older
        ``Full schema`` / ``Schema content`` names work too).
    path:
        Where to create the database: ':memory:' (default) or a file path
        that does not exist yet.

    Returns
    -------
    An open connection to the populated database.  DDL statements that are
    not valid SQLite and rows that violate a constraint are skipped.
    """
    row = _normalise(schema)
    on_disk = str(path) != ":memory:"
    if on_disk and Path(path).exists():
        raise FileExistsError(f"{path} already exists")

    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = OFF")
    if on_disk:  # a fresh file needs no rollback journal while it is filled
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")

    for statement in _split_ddl(row.get("full_schema") or ""):
        try:
            conn.execute(statement)
        except sqlite3.Error:
            pass

    content = _parse_json_field(row.get("schema_content"), default={})
    if isinstance(content, dict):
        for table, table_rows in content.items():
            _insert_rows(conn, table, table_rows)
    conn.commit()

    if on_disk:
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = FULL")
    return conn


def is_usable_question(text: Optional[str]) -> bool:
    """False for a missing phrasing or a placeholder such as '...' or '<string>'."""
    return isinstance(text, str) and not _PLACEHOLDER.fullmatch(text.strip())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise(row) -> dict:
    """Return *row* as a dict with the current column names."""
    return {_LEGACY_COLUMNS.get(k, k): v for k, v in dict(row).items()}


def _parse_json_field(raw, *, default):
    """Parse a JSON string field; return *default* on failure."""
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str) and raw:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            pass
    return default


def _parse_questions(raw) -> dict:
    questions = _parse_json_field(raw, default={})
    if not isinstance(questions, dict):
        return {}
    return {_LEGACY_STYLES.get(k, k): v for k, v in questions.items()}


def _iter_rows(source: str, split: str) -> Iterator[dict]:
    """Yield the rows of *split* from a local file/directory or a HuggingFace repo."""
    p = Path(source).expanduser()
    if p.exists():
        for f in _local_files(p, split):
            yield from _iter_file(f)
        return
    if p.suffix in (".parquet", ".arrow") or source.startswith((".", "/", "~")):
        raise FileNotFoundError(f"No such file or directory: {source}")
    yield from _iter_hub(source, split)


def _iter_hub(repo: str, split: str) -> Iterator[dict]:
    """Yield the rows of a HuggingFace dataset split, one parquet shard at a time.

    Each shard is downloaded into the HuggingFace cache and read in batches.
    Streaming the split through ``datasets`` instead leaves Arrow reader
    threads waiting when the loop stops early on the large schema rows, and
    the interpreter then hangs at exit.
    """
    try:
        from huggingface_hub import HfApi, hf_hub_download  # type: ignore
        files = HfApi().list_repo_files(repo, repo_type="dataset")
    except Exception as exc:
        raise ValueError(f"Could not load split '{split}' of '{repo}': {exc}") from exc

    pattern = re.compile(rf"(?:^|/){re.escape(split)}(?:-[^/]*)?\.(?:parquet|arrow)$")
    shards = sorted(f for f in files if pattern.search(f))
    if not shards:  # no split-named files: let datasets resolve the layout
        try:
            from datasets import load_dataset  # type: ignore
            dataset = load_dataset(repo, split=split, streaming=True)
        except Exception as exc:
            raise ValueError(f"Could not load split '{split}' of '{repo}': {exc}") from exc
        yield from dataset
        return
    for shard in shards:
        yield from _iter_file(Path(hf_hub_download(repo, shard, repo_type="dataset")))


def _local_files(p: Path, split: str) -> list[Path]:
    """Files of *split* inside *p*, or *p* itself when it is a file."""
    if p.is_file():
        return [p]
    files: list[Path] = []
    for pattern in (f"{split}-*.parquet", f"{split}.parquet", f"{split}-*.arrow", f"{split}.arrow"):
        files.extend(sorted(p.rglob(pattern)))
    if not files:  # a directory of unsplit files, as in the older releases
        files = sorted(p.glob("*.parquet")) + sorted(p.glob("*.arrow"))
    if not files:
        raise FileNotFoundError(f"No parquet/arrow files for split '{split}' in {p}")
    return files


def _iter_file(path: Path) -> Iterator[dict]:
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        for batch in pq.ParquetFile(str(path)).iter_batches(batch_size=16):
            yield from batch.to_pylist()
    elif path.suffix == ".arrow":
        from datasets import Dataset  # type: ignore

        yield from Dataset.from_file(str(path))
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


def _split_ddl(ddl: str) -> list[str]:
    return [s.strip() for s in re.split(r";\s*", ddl) if s.strip()]


def _quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _insert_rows(conn: sqlite3.Connection, table: str, table_rows) -> None:
    if not isinstance(table_rows, list):
        return
    table_rows = [r for r in table_rows if isinstance(r, dict)]
    if not table_rows:
        return
    cols = list(table_rows[0].keys())
    insert_sql = (
        f"INSERT OR IGNORE INTO {_quote(table)} ({', '.join(map(_quote, cols))}) "
        f"VALUES ({', '.join('?' * len(cols))})"
    )
    for row_dict in table_rows:
        try:
            conn.execute(insert_sql, _coerce_row(row_dict, cols))
        except (sqlite3.Error, OverflowError):
            pass


def _coerce_row(row_dict: dict, cols: list[str]) -> list:
    """Order a row's values by *cols* and convert them to SQLite-storable types."""
    values = []
    for c in cols:
        val = row_dict.get(c)
        if isinstance(val, (dict, list)):
            val = json.dumps(val)
        elif isinstance(val, int) and not isinstance(val, bool):
            val = max(_INT64_MIN, min(_INT64_MAX, val))
        elif isinstance(val, float):
            val = max(-_FLOAT_MAX, min(_FLOAT_MAX, val))
        values.append(val)
    return values


def _write_database(row: dict, schema_id: str, out: Path) -> dict:
    safe_id = re.sub(r"[^\w\-]", "_", schema_id)
    db_path = out / f"{safe_id}.db"
    try:
        for leftover in (db_path, *(db_path.with_name(db_path.name + s) for s in ("-wal", "-shm", "-journal"))):
            if leftover.exists():
                leftover.unlink()
        conn = build_database(row, db_path)
        try:
            rows_per_table = _count_rows(conn)
        finally:
            conn.close()
        error = None
    except Exception as exc:
        rows_per_table = {}
        error = str(exc)
    return {
        "schema_id": schema_id,
        "db_path": str(db_path.resolve()),
        "tables": list(rows_per_table),
        "rows_per_table": rows_per_table,
        "error": error,
    }


def _count_rows(conn: sqlite3.Connection) -> dict[str, int]:
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
    )]
    return {t: conn.execute(f"SELECT COUNT(*) FROM {_quote(t)}").fetchone()[0] for t in tables}


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Materialize the SQaLe databases as populated SQLite .db files."
    )
    p.add_argument(
        "--input",
        default=SCHEMAS_REPO,
        help=f"HuggingFace repo ID, local parquet/arrow file, or directory (default: {SCHEMAS_REPO}).",
    )
    p.add_argument(
        "--split",
        default="train",
        help="Dataset split to read: train or test (default: train).",
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
        help="Maximum number of databases to write.",
    )
    p.add_argument(
        "--schema-id",
        action="append",
        dest="schema_ids",
        metavar="ID",
        help="Only write this database; repeat for several.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    results = deserialize_sqale(
        file_path=args.input,
        output_dir=args.output,
        limit=args.limit,
        split=args.split,
        schema_ids=args.schema_ids,
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
