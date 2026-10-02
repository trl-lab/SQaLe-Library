# SQaLe

[![PyPI Downloads](https://static.pepy.tech/personalized-badge/sqale?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/sqale)

A Python library for the [SQaLe](https://trl-lab.github.io/sqale) text-to-SQL dataset. It loads SQaLe's questions and turns its databases into populated SQLite files, ready for training, evaluation and benchmarking.

SQaLe pairs 1,408,056 natural-language questions with 176,761 distinct SQL queries over 9,259 populated databases built from real-world schemas. The schemas are large, with a median of 113 tables and 538 columns. The dataset is published as two Hugging Face datasets that join on `schema_id`:

| Dataset | Contents | Rows |
|---|---|:-:|
| [`trl-lab/sqale2_queries`](https://huggingface.co/datasets/trl-lab/sqale2_queries) | questions in eight phrasings, gold SQL, difficulty, the gold query's result | 177,377 |
| [`trl-lab/sqale2_schemas`](https://huggingface.co/datasets/trl-lab/sqale2_schemas) | the DDL and generated table rows of each database | 9,259 |

Both are split by schema into `train` (8,836 databases, 169,277 questions) and `test` (423 databases, 8,100 questions), so no test database appears in training.

## Installation

```bash
pip install SQaLe
```

## Quickstart

Load the test questions, write the databases they are asked over, and run a gold query:

```python
import sqlite3
from sqale import deserialize_sqale, load_questions

questions = load_questions(split="test", limit=100)
databases = deserialize_sqale(
    split="test",
    output_dir="./dbs",
    schema_ids={q["schema_id"] for q in questions},
)
db_path = {d["schema_id"]: d["db_path"] for d in databases}

q = questions[0]
conn = sqlite3.connect(db_path[q["schema_id"]])
print(q["questions"]["verbose"])
print(conn.execute(q["sql"]).fetchmany(5))
print(q["execution_result"][:5])  # the stored result of the gold query
```

## CLI

`sqale-extract` writes databases from `trl-lab/sqale2_schemas` as `.db` files:

```bash
# every test database
sqale-extract --split test --output ./dbs

# the first 100 training databases
sqale-extract --split train --output ./dbs --limit 100

# specific databases
sqale-extract --split test --output ./dbs --schema-id schema_012721 --schema-id schema_001147
```

## Python API

### `load_questions`

```python
from sqale import load_questions

questions = load_questions(
    file_path="trl-lab/sqale2_queries",  # HuggingFace repo ID or local path
    split="test",                        # "train" or "test"
    difficulty=["moderate", "hard"],     # optional filter
    limit=None,                          # optional
    drop_placeholders=True,              # replace "..." phrasings with None
)
```

Each entry holds one question:

| Field | Description |
|---|---|
| `question_id` | unique question identifier |
| `schema_id` | the database the question is asked over |
| `difficulty` | `simple`, `moderate` or `hard` |
| `questions` | dict of phrasings keyed by style (`sqale.STYLES`): `verbose`, `evidence_supported`, `structured`, `requirements_list`, `short_ambiguous`, `short_high_level`, `casual`, `spelling_grammar_mistakes`; a value is `None` where no usable phrasing exists |
| `sql` | the gold SQL query (SQLite) |
| `relevant_tables` | tables of the subschema the question was generated from; the gold SQL uses a subset of them |
| `number_of_relevant_tables` | length of `relevant_tables` |
| `execution_result` | up to the first 50 rows of the gold query's result |

To train on every phrasing, flatten the questions into (question, SQL) pairs:

```python
pairs = [(text, q["sql"]) for q in questions for text in q["questions"].values() if text]
```

### `deserialize_sqale`

```python
from sqale import deserialize_sqale

results = deserialize_sqale(
    file_path="trl-lab/sqale2_schemas",  # HuggingFace repo ID or local path
    output_dir="./dbs",
    split="test",
    schema_ids=None,                     # optional: only these databases
    limit=None,                          # optional
)

for r in results:
    print(r["db_path"], r["rows_per_table"])
```

Each entry describes one database:

| Field | Description |
|---|---|
| `schema_id` | schema id from the dataset |
| `db_path` | absolute path to the created `.db` file |
| `tables` | tables created in the database |
| `rows_per_table` | dict mapping table name → number of rows it holds |
| `error` | error message if the database could not be written (or the requested `schema_id` is not in the split), otherwise `None` |

The databases are plain SQLite files without a write-ahead log, so they can be opened read-only and copied as single files.

### `build_database`

Builds one database from a row of `trl-lab/sqale2_schemas`, in memory or as a file:

```python
from datasets import load_dataset
from sqale import build_database

schemas = load_dataset("trl-lab/sqale2_schemas", split="test")
conn = build_database(schemas[0])                      # in memory
build_database(schemas[0], "schema_001147.db").close()  # as a file
```

### Placeholder phrasings

87,365 of the 1,408,056 phrasings in the release are placeholders left by the style-variation step, such as `...`, `<string>` or a bare difficulty label. `load_questions` replaces them with `None` unless `drop_placeholders=False`. `sqale.is_usable_question(text)` applies the same check to any string. Over both splits, 1,320,691 phrasings are usable.

## Local files

Every function also reads local copies, which is faster than streaming when you load a split more than once:

```bash
hf download trl-lab/sqale2_schemas --repo-type dataset --include "data/test-*" --local-dir ./sqale2_schemas
```

```python
results = deserialize_sqale(file_path="./sqale2_schemas", split="test", output_dir="./dbs")
questions = load_questions(file_path="./data/test-00000-of-00001.parquet")  # a single file ignores split
```

A directory is searched for `<split>-*.parquet` or `<split>-*.arrow` files. Supported formats are `.parquet` and `.arrow`. Files in the older single-table layout of `trl-lab/SQaLe_2` (columns such as `schema id`, `Full schema` and `sql statament`) are still read.

With a Hugging Face repo ID, the split's parquet shards are downloaded one at a time into the Hugging Face cache, so a later run reads them from disk. A run that stops early, through `limit` or `schema_ids`, downloads only the shards it reaches. The test split of the databases is about 180 MB and the train split about 4 GB. Writing a test database takes about 30 ms.

## Running a model on the databases

The three text-to-SQL agents from the SQaLe paper are in the [SQaLe collection](https://huggingface.co/collections/trl-lab/sqale-project). Each model repository includes `sqale_agent.py`, which runs the model on any SQLite file, including the databases written by this library:

```bash
python sqale_agent.py --model trl-lab/qwen3.5-2b-grpo-sqale --db ./dbs/schema_012721.db \
  --question "For each planet that has had a 'Trading Halt' alert, list the planet ID, the alert ID, and the total number of historical events recorded for that planet."
```

## Requirements

- Python ≥ 3.9
- `tqdm`, `pyarrow`, `huggingface_hub`, `datasets`

## Citation

```bibtex
@misc{wolff2026sqale,
  title  = {{SQaLe}: A Large Realistic Dataset to Empower Small Specialised Text-to-{SQL} Models},
  author = {Wolff, Cornelius and Gomm, Daniel and Hulsebos, Madelon},
  year   = {2026}
}
```

## License

See [LICENSE](LICENSE).
