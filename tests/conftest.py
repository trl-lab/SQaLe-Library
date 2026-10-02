"""Shared fixtures for the sqale test suite."""

import json
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

SAMPLE_QUESTIONS = [
    {
        "verbose (original)": "How many users are there?",
        "short_high_level": "Count all users.",
        "casual": "How many users?",
    },
    {
        "verbose (original)": "What is the total order amount?",
        "short_high_level": "Sum all order amounts.",
        "casual": "Total orders?",
    },
]

SAMPLE_SQLS = [
    "SELECT COUNT(*) FROM users",
    "SELECT SUM(amount) FROM orders",
]

STYLES = [
    "verbose", "evidence_supported", "structured", "requirements_list",
    "short_ambiguous", "short_high_level", "casual", "spelling_grammar_mistakes",
]


def _phrasings(text: str, **overrides) -> dict:
    """All eight phrasings of a question, derived from its verbose form."""
    questions = {style: f"{text} ({style})" for style in STYLES}
    questions["verbose"] = text
    questions.update(overrides)
    return questions


def _write_splits(root: Path, splits: dict) -> Path:
    (root / "data").mkdir(parents=True)
    for split, rows in splits.items():
        pd.DataFrame(rows).to_parquet(root / "data" / f"{split}-00000-of-00001.parquet", index=False)
    return root


@pytest.fixture()
def schemas_repo(tmp_path: Path) -> Path:
    """A local copy of a trl-lab/sqale2_schemas-shaped repo with train and test splits."""
    train = [
        {
            "schema_id": "schema_001",
            "full_schema": SAMPLE_DDL,
            "schema_content": json.dumps(SAMPLE_CONTENT),
            "number_of_tables": 2,
        },
        {
            "schema_id": "schema_002",
            "full_schema": "CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT); CREATE TABLE empty_one (id INTEGER)",
            "schema_content": json.dumps({"things": [{"id": 1, "label": "foo"}, {"id": 2, "label": "bar"}]}),
            "number_of_tables": 2,
        },
    ]
    test = [
        {
            "schema_id": "schema_101",
            "full_schema": "CREATE TABLE t (id INTEGER PRIMARY KEY, meta TEXT, big INTEGER, \"odd \"\"name\"\"\" TEXT)",
            "schema_content": json.dumps({"t": [
                {"id": 1, "meta": {"a": [1, 2]}, "big": 2 ** 70, "odd \"name\"": "x"},
                {"id": 1, "meta": None, "big": 0, "odd \"name\"": "duplicate key, ignored"},
            ]}),
            "number_of_tables": 1,
        },
    ]
    return _write_splits(tmp_path / "sqale2_schemas", {"train": train, "test": test})


@pytest.fixture()
def queries_repo(tmp_path: Path) -> Path:
    """A local copy of a trl-lab/sqale2_queries-shaped repo with train and test splits."""
    train = [
        {
            "question_id": "schema_001_s0_q1_aaaa",
            "schema_id": "schema_001",
            "sql": SAMPLE_SQLS[0],
            "difficulty": "simple",
            "questions": _phrasings("How many users are there?", casual="...", structured=None),
            "relevant_tables": json.dumps(["users", "orders"]),
            "number_of_relevant_tables": 2,
            "execution_result": json.dumps([[2]]),
        },
        {
            "question_id": "schema_001_s0_q2_bbbb",
            "schema_id": "schema_001",
            "sql": SAMPLE_SQLS[1],
            "difficulty": "moderate",
            "questions": _phrasings("What is the total order amount?", short_ambiguous="<string>"),
            "relevant_tables": json.dumps(["orders"]),
            "number_of_relevant_tables": 1,
            "execution_result": json.dumps([[149.49]]),
        },
        {
            "question_id": "schema_002_s0_q1_cccc",
            "schema_id": "schema_002",
            "sql": "SELECT label FROM things WHERE id = 2",
            "difficulty": "hard",
            "questions": _phrasings("Which label does thing 2 have?", verbose="**Hard**"),
            "relevant_tables": json.dumps(["things"]),
            "number_of_relevant_tables": 1,
            "execution_result": json.dumps([["bar"]]),
        },
    ]
    test = [
        {
            "question_id": "schema_101_s0_q1_dddd",
            "schema_id": "schema_101",
            "sql": "SELECT COUNT(*) FROM t",
            "difficulty": "simple",
            "questions": _phrasings("How many rows does t have?"),
            "relevant_tables": json.dumps(["t"]),
            "number_of_relevant_tables": 1,
            "execution_result": json.dumps([[1]]),
        },
    ]
    return _write_splits(tmp_path / "sqale2_queries", {"train": train, "test": test})


@pytest.fixture()
def sample_parquet(tmp_path: Path) -> Path:
    """A parquet file in the older single-table layout (trl-lab/SQaLe_2)."""
    df = pd.DataFrame(
        [
            {
                "schema id": "schema_001",
                "Full schema": SAMPLE_DDL,
                "Schema content": json.dumps(SAMPLE_CONTENT),
                "question id": "q_0000001",
                "questions": json.dumps(SAMPLE_QUESTIONS[0]),
                "sql statament": SAMPLE_SQLS[0],
                "difficulty": "simple",
                "relevant tables": json.dumps(["users"]),
                "number of relevant tables": 1,
                "execution_result": json.dumps([[2]]),
            },
            {
                "schema id": "schema_002",
                "Full schema": "CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT)",
                "Schema content": json.dumps(
                    {"things": [{"id": 1, "label": "foo"}, {"id": 2, "label": "bar"}]}
                ),
                "question id": "q_0000002",
                "questions": json.dumps(SAMPLE_QUESTIONS[1]),
                "sql statament": SAMPLE_SQLS[1],
                "difficulty": "moderate",
                "relevant tables": json.dumps(["orders"]),
                "number of relevant tables": 1,
                "execution_result": json.dumps([[149.49]]),
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
