"""
Tests for the sqale-extract CLI entry point.

Run with:  pytest tests/test_cli.py -v
"""

import subprocess
import sys


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


def test_cli_help():
    result = run_cli("--help")
    assert result.returncode == 0
    for flag in ("--input", "--split", "--output", "--limit", "--schema-id"):
        assert flag in result.stdout
    assert "trl-lab/sqale2_schemas" in result.stdout


def test_cli_basic(schemas_repo, output_dir):
    result = run_cli("--input", str(schemas_repo), "--output", str(output_dir))
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "Done: 2/2 succeeded" in result.stdout


def test_cli_split(schemas_repo, output_dir):
    result = run_cli("--input", str(schemas_repo), "--split", "test", "--output", str(output_dir))
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "1/1" in result.stdout
    assert [p.name for p in output_dir.glob("*.db")] == ["schema_101.db"]


def test_cli_limit(schemas_repo, output_dir):
    result = run_cli("--input", str(schemas_repo), "--output", str(output_dir), "--limit", "1")
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "1/1" in result.stdout


def test_cli_schema_ids(schemas_repo, output_dir):
    result = run_cli(
        "--input", str(schemas_repo), "--output", str(output_dir),
        "--schema-id", "schema_002", "--schema-id", "schema_404",
    )
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "1/2 succeeded" in result.stdout
    assert "FAIL schema_404" in result.stdout
    assert [p.name for p in output_dir.glob("*.db")] == ["schema_002.db"]


def test_cli_creates_db_files(schemas_repo, output_dir):
    run_cli("--input", str(schemas_repo), "--output", str(output_dir))
    db_files = sorted(p.name for p in output_dir.glob("*.db"))
    assert db_files == ["schema_001.db", "schema_002.db"]


def test_cli_legacy_file(sample_parquet, output_dir):
    result = run_cli("--input", str(sample_parquet), "--output", str(output_dir))
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "2/2" in result.stdout


def test_cli_invalid_input(output_dir):
    result = run_cli("--input", "/nonexistent/path/data.parquet", "--output", str(output_dir))
    assert result.returncode != 0
    assert "No such file or directory" in result.stderr


def test_cli_module_entry_point(schemas_repo, output_dir):
    result = run_cli_module("--input", str(schemas_repo), "--output", str(output_dir), "--limit", "1")
    assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
    assert "1/1" in result.stdout
