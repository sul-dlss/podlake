# podlake

[![Tests](https://github.com/sul-dlss/podlake/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss/podlake/actions/workflows/test.yml)

podlake harvests MARC XML data from [POD]'s OAI-PMH service, converts it to
Parquet with [marctable], and loads it into a [DuckLake] lakehouse so it can be
queried with DuckDB.

DuckLake is a DuckDB-centric table format: today you query it with DuckDB (the
`ducklake` extension). The data files are ordinary Parquet, but reading them
directly with another tool bypasses the DuckLake catalog — its snapshots and
merge-on-read delete files — and would return incorrect results after updates,
so use a DuckLake-aware client. Multi-engine access (Spark, Trino/Athena) is on
DuckLake's roadmap but not yet practical; if you need that today, Iceberg is the
better-supported format.

The data flows in three steps:

1. **harvest + convert** — `podlake convert` / `convert-all` pull MARC records
   from POD and write Parquet files (one per organization).
2. **load** — `podlake load` loads those Parquet files into a single DuckLake
   `records` table, partitioned by organization.
3. **query** — analysts attach to the DuckLake read-only and run SQL.

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

## Harvest and convert

List the organizations (OAI sets) available:

```
$ uvx podlake sets
```

Harvest a single provider to a Parquet file:

```
$ uvx podlake convert stanford stanford.parquet
```

Harvest every provider to its own Parquet file in a directory. By default this
runs one worker at a time; use `--workers` to harvest concurrently:

```
$ uvx podlake convert-all ./output/ --workers 4
```

Harvesting buffers records in memory per Parquet row group. Because the MARC
schema is very wide, the default (`--batch-size 100000`) can use several GB of
RAM for a large provider; lower it on memory-constrained machines:

```
$ uvx podlake convert stanford stanford.parquet --batch-size 10000
```

## Build the lake

Load the Parquet files into the DuckLake. Point `load` at a directory to load
every `*.parquet` (using each file name as the organization), or at a single
file:

```
$ uvx podlake load ./output/
$ uvx podlake load stanford.parquet --org stanford
```

Everything lands in one `records` table partitioned by `org`. Loading an
organization that is already present replaces its rows, so `load` is safe to
re-run.

## Keep the lake up to date

Once the lake exists, harvest only what has changed instead of rebuilding it.
`update` harvests the records an organization has changed since it was last
harvested, upserts them into `records` (keyed by `pod_record_id`), removes any
records POD reports as deleted, and records today as the org's new last-harvest
date — all in a single transaction, so each update is one DuckLake snapshot.

```
$ uvx podlake update stanford      # one organization
$ uvx podlake update-all           # every organization, one at a time
```

The last-harvest date for each org is stored in the lake itself, in a
`harvest_state` table. The first `update` for an org (with no recorded date)
does a full harvest to establish the baseline; subsequent runs are deltas. You
can override the start date with `--since`:

```
$ uvx podlake update stanford --since 2026-04-01
```

Run `update-all` on a schedule (cron, a systemd timer, a Kubernetes CronJob, or
GitHub Actions) to keep the lake current.

> **Note on deletions.** POD's OAI-PMH service reports deletions only
> *transiently* (`deletedRecord: transient`), so deletions are reliably applied
> as long as you update regularly. If updates lapse for a long time, reconcile
> by re-harvesting from scratch (`convert` / `convert-all` then `load`, or an
> `update --since` covering the gap).

## Publish for read-only consumers

To share the lake with other institutions read-only, maintain it locally with
the `development` profile and publish it to an S3 bucket. `publish` uploads the
catalog file and all Parquet data, so consumers can attach to it over `s3://`
with no database to reach — a good fit for a periodically-updated public
dataset:

```
$ uvx podlake publish s3://your-bucket/pod   # or set PODLAKE_PUBLISH_URL
```

A typical weekly cycle is `update-all` then `publish` (run on whatever schedule
you like). Because publishing is a plain upload, the maintainer's writable lake
never needs to be reachable by consumers — only the bucket does. See "Query the
lake" for how consumers attach.

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

The `test_oai` and `test_convert` tests perform live harvests against POD and
require `PODBUCKET_POD_TOKEN`. The `test_lake` tests run entirely locally against
a temporary development-profile lake.

[POD]: https://pod.stanford.edu/
[uv]: https://docs.astral.sh/uv/
[marctable]: https://github.com/sul-dlss-labs/marctable
[DuckLake]: https://ducklake.select/
