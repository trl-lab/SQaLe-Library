"""
Tests for the sqale-extract CLI entry point.

Run with:  pytest tests/test_cli.py -v
"""

import subprocess
import sys
from pathlib import Path


def run_cli(*args: str) -> subprocess.CompletedProcess:
    """Run sqale-extract via the installed console script."""
    return subprocess.run(
        ["sqale-extract", *args],
        capture_output=True,
        text=True,
    )


def run_cli_module(*args: str) -> subprocess.CompletedProcess:
    """Fallback: run sqale.deserialize as a module (works without install)."""
    return subprocess.run(
        [sys.executable, "-m", "sqale.deserialize", *args],
        capture_output=True,
        text=True,
    )


def test_cli_missing_input():
    result = run_cli()
    assert result.returncode != 0
    assert "required" in result.stderr.lower() or "error" in result.stderr.lower()


def test_cli_basic(sample_parquet, output_dir):
    result = run_cli(
        "--input", str(sample_parquet),
        "--output", str(output_dir),
    )
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "Done:" in result.stdout
    assert "2/2" in result.stdout


def test_cli_limit(sample_parquet, output_dir):
    result = run_cli(
        "--input", str(sample_parquet),
        "--output", str(output_dir),
        "--limit", "1",
    )
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "1/1" in result.stdout


def test_cli_creates_db_files(sample_parquet, output_dir):
    run_cli(
        "--input", str(sample_parquet),
        "--output", str(output_dir),
    )
    db_files = list(output_dir.glob("*.db"))
    assert len(db_files) == 2, f"Expected 2 .db files, found: {db_files}"


def test_cli_invalid_input(output_dir):
    result = run_cli(
        "--input", "/nonexistent/path/data.parquet",
        "--output", str(output_dir),
    )
    assert result.returncode != 0


def test_cli_help():
    result = run_cli("--help")
    assert result.returncode == 0
    assert "--input" in result.stdout
    assert "--output" in result.stdout
    assert "--limit" in result.stdout
