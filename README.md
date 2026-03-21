# SQaLe

[![PyPI Downloads](https://static.pepy.tech/personalized-badge/sqale?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/sqale)

A Python utility for deserializing the [SQaLe dataset](https://huggingface.co/datasets/trl-lab/SQaLe_2) into populated SQLite databases.

Each unique schema in the dataset is materialized as a `.db` file and populated with the synthetic row data stored alongside it — ready to use for SQL benchmarking, evaluation, or development.

## Installation

```bash
pip install SQaLe
```

## Usage

### CLI

```bash
# Download and deserialize all schemas
sqale-extract --output ./dbs

# Limit to the first 100 unique schemas
sqale-extract --output ./dbs --limit 100
```

### Python API

```python
from sqale import deserialize_sqale

results = deserialize_sqale(
    file_path="trl-lab/SQaLe_2",  # HuggingFace repo ID or local path
    output_dir="./dbs",
    limit=100,  # optional
)

for r in results:
    print(r["db_path"], r["rows_per_table"])
```

The function returns a list of dicts with the following fields:

| Field | Description |
|---|---|
| `schema_id` | Original schema ID from the dataset |
| `db_path` | Absolute path to the created `.db` file |
| `tables` | List of table names found in the DDL |
| `rows_per_table` | Dict mapping table name → number of rows inserted |
| `error` | Error message if materialization failed, otherwise `None` |

### Loading from a local file

```python
results = deserialize_sqale(
    file_path="./data/train.parquet",
    output_dir="./dbs",
)
```

Supported local formats: `.parquet`, `.arrow`, or a directory containing either.

## Requirements

- Python ≥ 3.9
- `pandas`, `tqdm`, `pyarrow`, `datasets`

## License

See [LICENSE](LICENSE).
