# AGENTS.md

Guidance for agents (and humans) working in podlake.

## What podlake is

A command-line tool that harvests MARC XML from POD's OAI-PMH service, converts
it to Parquet with marctable, loads it into a DuckLake store, keeps it current
with incremental updates, and can publish it to S3 for read-only consumers. See
`README.md` for the user-facing workflow.

## Checks to run before opening or reviewing a PR

Run all three and make sure they are clean, with no new warnings:

```sh
uv run ruff format      # format the code
uv run ruff check .     # lint
uv run ty check .        # type check
uv run pytest           # tests
```

`test_oai` and `test_convert` make live calls to POD and need
`PODBUCKET_POD_TOKEN`; the DuckLake and storage tests run fully locally.

## Code review checklist

Review a change for:

- **Simplicity.** Prefer the simplest code that does the job. Flag complexity
  that can be removed.
- **Subtlety needs comments.** Complex-but-necessary code must document *why*
  it is that way (not *what* it does).
- **Redundancy.** Duplicated or near-duplicated code that could be unified.
- **Security.** Possible vulnerabilities — injection (SQL, shell), credential
  handling and leakage, unsafe deserialization, path traversal.
- **Dead code.** Code that was once used but no longer is.
- **Test sufficiency.** Do unit and integration tests adequately cover the new
  code and its edge cases — error paths, fallbacks, empty/boundary inputs?
- **Docs.** Anything needing an update: `README.md`, `AGENTS.md`, CLI `--help`
  text, or code comments.

Also worth checking:

- **Correctness & edge cases.** Error handling, empty/boundary inputs,
  fallbacks.
- **Scope.** No accidental scope creep, and no scratch/data files committed
  (harvested Parquet, `*.ducklake`, `lake-data/`, `test.log` are all gitignored
  — keep it that way).
- **Build hygiene.** ruff/ty/pytest clean, no new warnings.
- **Spec adherence.** Follow the conventions of the systems podlake bridges:
  OAI-PMH (sets, `from` date granularity, `deletedRecord` semantics), MARC /
  MARCXML (control vs data fields, the 001 as record id), and DuckLake
  (snapshots, partitioning, merge-on-read delete files, read-only attach).
