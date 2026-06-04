"""
Tests for the load_questions API.

Run with:  pytest tests/test_questions.py -v
"""

import json

import pandas as pd
import pytest

from sqale import load_questions


def test_basic_load(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet))
    assert len(questions) == 2


def test_question_fields(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet))
    q = questions[0]

    assert q["question_id"] == "q_0000001"
    assert q["schema_id"] == "schema_001"
    assert q["difficulty"] == "simple"
    assert isinstance(q["questions"], dict)
    assert "verbose (original)" in q["questions"]
    assert q["sql"] == "SELECT COUNT(*) FROM users"
    assert isinstance(q["relevant_tables"], list)
    assert isinstance(q["execution_result"], list)


def test_questions_parsed(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet))
    q = questions[0]
    assert q["questions"]["verbose (original)"] == "How many users are there?"
    assert q["questions"]["short_high_level"] == "Count all users."
    assert q["questions"]["casual"] == "How many users?"


def test_relevant_tables_parsed(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet))
    assert questions[0]["relevant_tables"] == ["users"]
    assert questions[1]["relevant_tables"] == ["orders"]


def test_execution_result_parsed(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet))
    assert questions[0]["execution_result"] == [[2]]


def test_limit(sample_parquet):
    questions = load_questions(file_path=str(sample_parquet), limit=1)
    assert len(questions) == 1


def test_no_deduplication(tmp_path):
    """load_questions should return all rows, even with duplicate schema_ids."""
    df = pd.DataFrame(
        [
            {
                "schema id": "schema_dup",
                "Full schema": "CREATE TABLE t (id INTEGER)",
                "Schema content": "{}",
                "question id": "q_0000010",
                "questions": json.dumps({"verbose (original)": "Q1?"}),
                "sql statament": "SELECT 1",
                "difficulty": "simple",
                "relevant tables": "[]",
                "number of relevant tables": 0,
                "execution_result": "[[1]]",
            },
            {
                "schema id": "schema_dup",
                "Full schema": "CREATE TABLE t (id INTEGER)",
                "Schema content": "{}",
                "question id": "q_0000011",
                "questions": json.dumps({"verbose (original)": "Q2?"}),
                "sql statament": "SELECT 2",
                "difficulty": "simple",
                "relevant tables": "[]",
                "number of relevant tables": 0,
                "execution_result": "[[2]]",
            },
        ]
    )
    pq = tmp_path / "dup.parquet"
    df.to_parquet(str(pq), index=False)

    questions = load_questions(file_path=str(pq))
    assert len(questions) == 2, "load_questions must not deduplicate by schema_id"
    ids = {q["question_id"] for q in questions}
    assert ids == {"q_0000010", "q_0000011"}
