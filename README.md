# podlake

[![Tests](https://github.com/sul-dlss/podlake/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss/podlake/actions/workflows/test.yml)

podlake syncs MARC XML data from [POD]'s [ResourceSync] service, converts it to
Parquet with [marctable], and loads it into a [DuckLake] lakehouse so it can be
queried with DuckDB.

DuckLake is a DuckDB-centric table format: today you query it with DuckDB (the
`ducklake` extension). The data files are ordinary Parquet, but reading them
directly with another tool bypasses the DuckLake catalog — its snapshots and
merge-on-read delete files — and would return incorrect results after updates,
so use a DuckLake-aware client. Multi-engine access (Spark, Trino/Athena) is on
DuckLake's roadmap but not yet practical; if you need that today, Iceberg is the
better-supported format.

The data flows in two steps:

1. **sync** — `podlake sync` / `sync-all` download POD's ResourceSync dump files
   (a base full dump plus a chain of daily delta and delete files), convert them
   to Parquet, and upsert them into a single DuckLake `records` table,
   partitioned by organization.
2. **query** — analysts attach to the DuckLake read-only and run SQL.

podlake uses ResourceSync rather than POD's OAI-PMH endpoint because the dump
files are static downloads (fast, no server-side paging) and carry explicit,
durable deletions. A consequence is that the lake is only as current as POD's
most recently published delta.

## Install

Install [uv], then run podlake with `uvx`:

```
$ uvx podlake --help
```

## Configure

podlake is configured with environment variables, which it reads from a `.env`
file in the current directory (or the real environment). Copy the token you use
for POD into `PODBUCKET_POD_TOKEN`, then choose a profile with `PODLAKE_ENV`.

There are two profiles:

**`development` (default)** — a local DuckDB catalog file and local Parquet.
Nothing external is required, so it's ideal for experimenting. It's also the
profile you use to maintain a lake locally and then `publish` it to S3 for
read-only consumers (see below):

```sh
PODBUCKET_POD_TOKEN=your-pod-token
PODLAKE_ENV=development
PODLAKE_CATALOG=podlake.ducklake         # local catalog file (default)
PODLAKE_DATA_PATH=./lake-data/           # where Parquet data files live (default)
PODLAKE_PUBLISH_URL=s3://your-bucket/pod # optional: default target for `publish`
```

**`production`** — a Postgres catalog and an S3 data path, for a shared lake
that many analysts can query and write *concurrently*. Most read-only sharing
does not need this — prefer the `publish` workflow below unless you need
concurrent writers or many-times-a-day live updates:

```sh
PODBUCKET_POD_TOKEN=your-pod-token
PODLAKE_ENV=production
PODLAKE_DATA_PATH=s3://your-bucket/lake/
PODLAKE_PG_HOST=your-db-host
PODLAKE_PG_DBNAME=podlake
PODLAKE_PG_USER=podlake
PODLAKE_PG_PASSWORD=...
# or, instead of the PODLAKE_PG_* vars, a single DSN:
# PODLAKE_PG_DSN=host=... dbname=podlake user=podlake password=...

# AWS credentials for S3 are resolved by DuckDB's credential_chain, i.e. the
# standard AWS_* environment variables, your shared config, or an assumed role.
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-west-2
```

Check the resolved profile and confirm podlake can reach the lake:

```
$ uvx podlake config
```

## Sync from POD

List the organizations (ResourceSync streams) available, with how many resources
(full dump + deltas + deletes) each has and their total download size:

```
$ uvx podlake streams
```

Sync an organization into the DuckLake:

```
$ uvx podlake sync stanford      # one organization
$ uvx podlake sync-all           # every organization, one at a time
```

`sync` processes every ResourceSync resource newer than the org's cursor, in
chronological order: the base full dump, then each daily delta (upserted into
`records` by `pod_record_id`) and delete file (removed). The **first run does
the full initial load; later runs apply only new deltas** — one command for both.
Each resource is applied in its own transaction (one DuckLake snapshot) and
advances the org's cursor (stored in the lake's `harvest_state` table), so an
interrupted `sync` resumes cleanly from where it left off. Run `sync-all` on a
schedule (cron, a systemd timer, a Kubernetes CronJob, or GitHub Actions) to
keep the lake current.

Deletions are explicit and durable in ResourceSync (POD publishes delete files),
so no records linger after they are removed upstream.

Syncing buffers records in memory per Parquet row group. Because the MARC schema
is very wide, the default (`--batch-size 100000`) can use several GB of RAM on a
large full dump; lower it on memory-constrained machines. Dumps are streamed and
temporary files are cleaned up as each resource is processed, so peak disk use is
roughly one resource at a time, not the whole chain:

```
$ uvx podlake sync stanford --batch-size 10000
```

## Fetch raw Parquet (data collection)

To inspect the converted data without loading it into a DuckLake, `fetch`
downloads and converts an organization's ResourceSync dumps to Parquet files on
disk and stops there — no lake, no cursor, none of the load cost. Use
`--full-only` to grab just the base full dump:

```
$ uvx podlake fetch stanford ./out --full-only
```

Each MARCXML resource becomes a `.parquet` in the output directory (delete files
are copied as-is). This is handy for measuring things like column sparsity on
real data.

## Load pre-built Parquet (optional)

`sync` is the normal way to ingest. As an escape hatch, `load` ingests an
existing Parquet file (or a directory of per-org files, using each file name as
the organization) into the same `records` table:

```
$ uvx podlake load ./output/
$ uvx podlake load stanford.parquet --org stanford
```

Loading an organization that is already present replaces its rows, so `load` is
safe to re-run.

## Publish for read-only consumers

To share the lake with other institutions read-only, maintain it locally with
the `development` profile and publish it to an S3 bucket. `publish` uploads the
catalog file and all Parquet data, so consumers can attach to it over `s3://`
with no database to reach — a good fit for a periodically-updated public
dataset:

```
$ uvx podlake publish s3://your-bucket/pod   # or set PODLAKE_PUBLISH_URL
```

A typical cycle is `sync-all` then `publish` (run on whatever schedule you like).
Because publishing is a plain upload, the maintainer's writable lake never needs
to be reachable by consumers — only the bucket does. See "Query the lake" for how
consumers attach.

## Query the lake

For a quick check you can query through podlake, which connects read-only:

```
$ uvx podlake query "SELECT org, count(*) FROM records GROUP BY org"
```

Analysts typically connect directly with DuckDB. Attach the lake **read-only**
so a consumer connection can never modify it:

```sql
-- a published lake in a bucket (what most consumers use)
INSTALL ducklake; INSTALL httpfs;
ATTACH 'ducklake:s3://your-bucket/pod/podlake.ducklake' AS podlake
  (DATA_PATH 's3://your-bucket/pod/lake-data/', READ_ONLY, OVERRIDE_DATA_PATH true);
USE podlake;

-- a local development lake
INSTALL ducklake;
ATTACH 'ducklake:podlake.ducklake' AS podlake
  (DATA_PATH './lake-data/', READ_ONLY);
USE podlake;

-- a shared Postgres-catalog lake (the `production` profile)
INSTALL ducklake; INSTALL postgres; INSTALL httpfs;
ATTACH 'ducklake:postgres:dbname=podlake host=your-db-host user=... password=...'
  AS podlake (DATA_PATH 's3://your-bucket/lake/', READ_ONLY);
USE podlake;

-- records per organization
SELECT org, count(*) FROM records GROUP BY org;

-- consortial overlap: works held by more than one institution
SELECT goldrush_key, count(DISTINCT org) AS orgs
FROM records
GROUP BY goldrush_key
HAVING orgs > 1;
```

The `OVERRIDE_DATA_PATH true` on the published-lake attach is needed because the
catalog was written with a local data path and consumers re-root it at the
bucket. A public bucket needs no credentials; for a private bucket, consumers
supply read-only AWS credentials (e.g. via a `CREATE SECRET (TYPE s3, ...)`).

Because DuckLake uses snapshot isolation, the maintainer can update or republish
the lake while analysts keep querying — readers always see the last
fully-committed snapshot and can even pin a version for reproducibility with
`FROM records AT (VERSION => N)`.

The `READ_ONLY` flag stops a consumer *connection* from writing. For a stronger
guarantee, enforce it with credentials: for a published bucket, grant consumers
read-only S3 access (`s3:GetObject` / `s3:ListBucket`) and keep write access for
the maintainer; for the Postgres model, give analysts a `SELECT`-only role.

## Develop

Clone the repository, make changes, and run the tests:

```
$ uv run pytest
```

The tests run entirely locally (no network or `PODBUCKET_POD_TOKEN` needed):
ResourceSync manifest parsing is tested with fixtures, MARCXML conversion with
small in-test dumps, and the lake/publish paths against a temporary
development-profile lake (S3 is mocked with moto).

[POD]: https://pod.stanford.edu/
[ResourceSync]: https://www.openarchives.org/rs/toc
[uv]: https://docs.astral.sh/uv/
[marctable]: https://github.com/sul-dlss-labs/marctable
[DuckLake]: https://ducklake.select/
