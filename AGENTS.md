# AGENTS.md

Guidance for agents (and humans) working in podlake.

## What podlake is

A command-line tool that syncs MARC XML from POD's ResourceSync service (full
dump + daily deltas + delete files), parses it with pymarc/lxml, and loads it
into a DuckLake store as a tall/EAV `records` table (one row per subfield, plus
a `record_meta` table with `goldrush_key`). It keeps the lake current with
incremental syncs and can publish it to S3 for read-only consumers. See
`README.md` for the user-facing workflow.

Issues are tracked in **GitHub Issues** (https://github.com/sul-dlss/podlake/issues).

## Checks to run before opening or reviewing a PR

Run all three and make sure they are clean, with no new warnings:

```sh
uv run ruff format      # format the code
uv run ruff check .     # lint
uv run ty check .        # type check
uv run pytest           # tests
```

The tests run fully locally (no network or `PODBUCKET_POD_TOKEN`): ResourceSync
manifest parsing uses fixtures, conversion uses small in-test MARCXML dumps, and
the lake/publish paths use a temporary lake with moto-mocked S3.

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
  ResourceSync (capability/resource lists, full vs delta dumps, delete files,
  processing resources in `lastmod` order), MARC / MARCXML (leader, control vs
  data fields, indicators, the 001 as record id; the EAV `records` schema is
  meant to be lossless — preserve field/subfield order via `field_seq`/
  `subfield_seq`), and DuckLake (snapshots, partitioning, merge-on-read delete
  files, read-only attach).
