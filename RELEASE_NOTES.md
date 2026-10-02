# Release Notes

## v0.2.0 — The two-dataset SQaLe release

SQaLe is now published as two Hugging Face datasets that join on `schema_id`: [`trl-lab/sqale2_queries`](https://huggingface.co/datasets/trl-lab/sqale2_queries) (177,377 questions) and [`trl-lab/sqale2_schemas`](https://huggingface.co/datasets/trl-lab/sqale2_schemas) (9,259 databases), each with a schema-disjoint `train` / `test` split. This release reads that layout by default.

### What changed

- `deserialize_sqale()` reads `trl-lab/sqale2_schemas` and `load_questions()` reads `trl-lab/sqale2_queries` by default.
- Both take a `split` argument (`"train"` or `"test"`), and the CLI takes `--split`.
- `deserialize_sqale(schema_ids=...)` and the CLI's repeatable `--schema-id` write only the requested databases. Ids that are not in the split come back as results with an error.
- `load_questions(difficulty=...)` filters by difficulty, and `drop_placeholders=True` (the default) replaces placeholder phrasings such as `...` with `None`.
- Question entries gain `number_of_relevant_tables`, and their `questions` dict is keyed by the eight styles in `sqale.STYLES`.
- New public helpers: `build_database(schema, path)` builds one database in memory or as a file, and `is_usable_question(text)` detects placeholder phrasings.
- Databases are written one at a time and parquet files are read in batches, so memory use no longer grows with the size of the split.
- A HuggingFace repo ID is read shard by shard from the HuggingFace cache instead of being streamed through `datasets`. Streaming the schema rows could leave the interpreter hanging at exit after a run stopped early through `limit`. Repos without split-named shards still fall back to streaming.
- `.db` files are written without a write-ahead log, so opening them read-only no longer leaves `-wal` / `-shm` files behind.
- `rows_per_table` now counts the rows of every table created in the database, including empty ones.
- Nested JSON values in table rows are stored as JSON text instead of being dropped.
- `pandas` is no longer a direct dependency.

### Upgrade notes

- The original question is now under `questions["verbose"]` instead of `questions["verbose (original)"]`. Files in the older single-table layout are still read and mapped to the new keys.
- `split` defaults to `"train"`. Pass `split="test"` for evaluation.
- `--input` defaults to `trl-lab/sqale2_schemas`.
- With `drop_placeholders=True`, a phrasing can be `None`. Check for it before using a phrasing, or pass `drop_placeholders=False` to get the raw strings.

## v0.1.5 — Support for cwolff/data_work_in_progress + load_questions API

### What changed

- Default dataset updated from `trl-lab/SQaLe_2` to `cwolff/data_work_in_progress`.
- New `load_questions()` function returns question-level benchmark data (NL questions, gold SQL, difficulty, relevant tables, expected execution result) without schema deduplication.
- `--input` is now required in the CLI; pass the HuggingFace repo ID or a local file path explicitly.

### New API

```python
from sqale import load_questions

questions = load_questions("cwolff/data_work_in_progress", limit=100)
# Each entry contains:
#   question_id, schema_id, difficulty, questions (dict of formulations),
#   sql, relevant_tables, execution_result
```

`deserialize_sqale()` is unchanged — it still materializes unique schemas as SQLite `.db` files.

### Upgrade notes

The `--input` flag is now required on the CLI (no default). Users who relied on the implicit default must pass `--input cwolff/data_work_in_progress` explicitly.
