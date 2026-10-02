"""
Tests for the load_questions API.

Run with:  pytest tests/test_questions.py -v
"""

import json

import pandas as pd
import pytest

from sqale import STYLES, is_usable_question, load_questions


def test_basic_load(queries_repo):
    questions = load_questions(file_path=str(queries_repo))
    assert len(questions) == 3


def test_question_fields(queries_repo):
    q = load_questions(file_path=str(queries_repo))[0]

    assert q["question_id"] == "schema_001_s0_q1_aaaa"
    assert q["schema_id"] == "schema_001"
    assert q["difficulty"] == "simple"
    assert q["sql"] == "SELECT COUNT(*) FROM users"
    assert set(q["questions"]) == set(STYLES)
    assert q["questions"]["verbose"] == "How many users are there?"
    assert q["relevant_tables"] == ["users", "orders"]
    assert q["number_of_relevant_tables"] == 2
    assert q["execution_result"] == [[2]]


def test_placeholders_dropped(queries_repo):
    questions = load_questions(file_path=str(queries_repo))
    assert questions[0]["questions"]["casual"] is None          # "..."
    assert questions[0]["questions"]["structured"] is None      # missing
    assert questions[1]["questions"]["short_ambiguous"] is None  # "<string>"
    assert questions[2]["questions"]["verbose"] is None         # "**Hard**"
    assert questions[2]["questions"]["casual"] is not None


def test_placeholders_kept_on_request(queries_repo):
    questions = load_questions(file_path=str(queries_repo), drop_placeholders=False)
    assert questions[0]["questions"]["casual"] == "..."
    assert questions[2]["questions"]["verbose"] == "**Hard**"


def test_split_selection(queries_repo):
    questions = load_questions(file_path=str(queries_repo), split="test")
    assert [q["question_id"] for q in questions] == ["schema_101_s0_q1_dddd"]


def test_difficulty_filter(queries_repo):
    assert [q["difficulty"] for q in load_questions(file_path=str(queries_repo), difficulty="hard")] == ["hard"]
    both = load_questions(file_path=str(queries_repo), difficulty=["simple", "moderate"])
    assert [q["difficulty"] for q in both] == ["simple", "moderate"]


def test_limit(queries_repo):
    assert len(load_questions(file_path=str(queries_repo), limit=1)) == 1
    assert load_questions(file_path=str(queries_repo), limit=0) == []


def test_no_deduplication(queries_repo):
    """Two questions over the same schema are both returned."""
    questions = load_questions(file_path=str(queries_repo))
    assert [q["schema_id"] for q in questions].count("schema_001") == 2


@pytest.mark.parametrize("text, usable", [
    ("How many users are there?", True),
    ("...", False),
    ("  … ", False),
    ("", False),
    (None, False),
    ("<string>", False),
    ("**Moderate**", False),
    ("hard", False),
    ("Which hard drives failed?", True),
])
def test_is_usable_question(text, usable):
    assert is_usable_question(text) is usable


# ---------------------------------------------------------------------------
# Older single-table layout (trl-lab/SQaLe_2)
# ---------------------------------------------------------------------------

def test_legacy_layout(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet))
    assert len(questions) == 2
    q = questions[0]
    assert q["question_id"] == "q_0000001"
    assert q["schema_id"] == "schema_001"
    assert q["sql"] == "SELECT COUNT(*) FROM users"
    assert q["questions"]["verbose"] == "How many users are there?"  # was "verbose (original)"
    assert q["questions"]["short_high_level"] == "Count all users."
    assert q["relevant_tables"] == ["users"]
    assert q["execution_result"] == [[2]]


def test_legacy_no_deduplication(tmp_path):
    """load_questions should return all rows, even with duplicate schema_ids."""
    rows = [
        {
            "schema id": "schema_dup",
            "Full schema": "CREATE TABLE t (id INTEGER)",
            "Schema content": "{}",
            "question id": qid,
            "questions": json.dumps({"verbose (original)": text}),
            "sql statament": sql,
            "difficulty": "simple",
            "relevant tables": "[]",
            "number of relevant tables": 0,
            "execution_result": result,
        }
        for qid, text, sql, result in [
            ("q_0000010", "Q1?", "SELECT 1", "[[1]]"),
            ("q_0000011", "Q2?", "SELECT 2", "[[2]]"),
        ]
    ]
    pq = tmp_path / "dup.parquet"
    pd.DataFrame(rows).to_parquet(str(pq), index=False)

    questions = load_questions(file_path=str(pq))
    assert {q["question_id"] for q in questions} == {"q_0000010", "q_0000011"}
