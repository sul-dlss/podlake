# podlake

[![Tests](https://github.com/sul-dlss/podlake/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss/podlake/actions/workflows/test.yml)

podlake harvests MARC from [POD]'s [ResourceSync] service, converts it to
Parquet, and loads it into a [DuckLake] lakehouse so it can be queried with
DuckDB. The lake is only ever as current as POD's most recently published delta.

## The DuckLake schema

[DuckLake] is a DuckDB-centric table format: a catalog plus ordinary Parquet
data files. DuckLake handles record updates and deletes for you so you will
always get the most recent record. You query it with DuckDB (the `ducklake`
extension), instead of reading the Parquet files directly.

Once you connect to the DuckLake you will see two tables, both partitioned by
`org` (the contributing institution).

**`record_meta` — one row per record**

| column | type | notes |
| --- | --- | --- |
| `org` | VARCHAR | contributing institution (partition key) |
| `pod_record_id` | VARCHAR | stable record id, `org:localid` |
| `goldrush_key` | VARCHAR | Gold Rush match key; groups records into distinct titles |

**`records` — one row per subfield (tall / EAV)**

| column | type | notes |
| --- | --- | --- |
| `org` | VARCHAR | partition key |
| `pod_record_id` | VARCHAR | joins to `record_meta` |
| `field_tag` | VARCHAR | MARC tag; the leader is `'LDR'` |
| `field_seq` | INTEGER | field order within the record |
| `ind1`, `ind2` | VARCHAR | indicators (NULL for control fields) |
| `subfield_code` | VARCHAR | subfield code; NULL for control fields / leader |
| `subfield_seq` | INTEGER | subfield order within the field |
| `value` | VARCHAR | the subfield text (or the control-field / leader string) |

Notes for querying:

- The leader is `field_tag = 'LDR'`; control fields (00X) hold their data in
  `value` with a NULL `subfield_code`.
- Field and subfield order is preserved by `field_seq` / `subfield_seq`, so a
  record round-trips exactly.
- Handy 008 slices (1-indexed): publication year = `substr(value, 8, 4)`, place
  of publication = chars 16–18, language = 36–38; leader type-of-record = char 7.

(podlake also keeps a small internal `harvest_state` table — the per-org sync
cursor — which you don't normally query.)

This tall, one-row-per-subfield layout is deliberate, and follows the
MARC-in-Parquet format research in [`dchud/mrrc`][mrrc]: it loads into DuckLake
orders of magnitude faster and cheaper than a wide one-column-per-field table,
every field/subfield/indicator is uniformly queryable with a plain `WHERE`, and
it's lossless. As that evaluation observes, columnar storage pays off for very
large analytic collections — which is exactly podlake's case.

### Gold Rush keys

Every `record_meta` row carries a `goldrush_key`: a normalized key produced by
the Colorado Alliance's [Gold Rush match key][goldrush] algorithm, derived from a
record's title, author, publication year, edition, publisher, pagination,
material type, and carrier. Records that produce the same key describe the same
title, so the key is how you work at the *title* level across the consortium —
`GROUP BY goldrush_key` collapses many institutions' records for one title into a
single group, and `count(DISTINCT org)` per key is how many institutions hold it
(the basis for overlap, rarity / "last copies", and deduplication). It is roughly
*manifestation*-level: because it captures edition and carrier, a print book and
its e-book edition get distinct keys and won't group together.

### Example queries

```sql
-- records vs. distinct titles per organization
SELECT org, count(*) AS records, count(DISTINCT goldrush_key) AS titles
FROM record_meta GROUP BY org;

-- consortial overlap: titles held by more than one institution
SELECT goldrush_key, count(DISTINCT org) AS orgs
FROM record_meta GROUP BY goldrush_key HAVING orgs > 1;

-- all titles (245 $a)
SELECT value FROM records WHERE field_tag = '245' AND subfield_code = 'a';

-- pull several fields per record as columns (conditional aggregation)
SELECT pod_record_id,
  max(value) FILTER (WHERE field_tag = '245' AND subfield_code = 'a') AS title,
  max(value) FILTER (WHERE field_tag = '100' AND subfield_code = 'a') AS author
FROM records WHERE field_tag IN ('245', '100') GROUP BY pod_record_id;

-- reconstruct a record in order (leader first, then fields/subfields)
SELECT field_tag, ind1, ind2, subfield_code, value
FROM records WHERE pod_record_id = 'stanford:a1'
ORDER BY field_seq, subfield_seq;
```

### Joining fields and subfields

Because each subfield is its own row, you relate them with a **self-join** on
`records`. Join on `pod_record_id` to combine different fields of a record; join
on `(pod_record_id, field_seq)` to combine subfields of the *same* field
occurrence — something `FILTER`-aggregation can't distinguish when a field
repeats:

```sql
-- title ($a) paired with the remainder-of-title ($b) from the same 245
SELECT a.pod_record_id, a.value AS title, b.value AS remainder
FROM records a JOIN records b USING (pod_record_id, field_seq)
WHERE a.field_tag = '245' AND a.subfield_code = 'a' AND b.subfield_code = 'b';
```

The `FILTER (WHERE …)` form shown above is simpler when you just want one value
per field per record; reach for a self-join when a field can repeat (multiple
650s, 856s, …) or when you need to correlate subfields within a single field.

## Building the lake

Install [uv], then run podlake with `uvx`:

```
$ uvx podlake --help
```

Configure it with environment variables (read from a `.env` file or the
environment): put your POD token in `PODBUCKET_POD_TOKEN` and pick a profile with
`PODLAKE_PROFILE`. The default **`file`** profile uses a local catalog file and
local Parquet — ideal for building a lake locally and then publishing it:

```sh
PODBUCKET_POD_TOKEN=your-pod-token
PODLAKE_PROFILE=file
PODLAKE_CATALOG=podlake.ducklake         # local catalog file (default)
PODLAKE_DATA_PATH=./lake-data/           # where Parquet data files live (default)
PODLAKE_PUBLISH_URL=s3://your-bucket/pod # optional default target for `publish`
```

In production you run that same `file` profile on a server, `sync`, and
`publish` the file-catalog lake to S3 (below); POD members then attach to the
bucket read-only, with no database to run or expose. This suits a periodically
updated, read-mostly, widely-shared lake — the catalog is a small file in the
bucket, and S3 fans out to many readers far more easily than a database
connection would. (A Postgres-catalog **`postgres`** profile also exists, but
it's only worth the operational overhead for a lake with *concurrent writers* or
many-times-a-day live updates — not for read-only sharing.) Confirm the resolved
profile with `uvx podlake config`.

**Sync** downloads POD's ResourceSync dumps (a base full dump plus a chain of
daily delta and delete files), converts them to Parquet, and upserts them into
the lake:

```
$ uvx podlake streams            # list organizations + their resource counts/sizes
$ uvx podlake sync stanford      # one organization
$ uvx podlake sync-all           # every organization, one at a time
```

The **first run does the full initial load; later runs apply only new deltas** —
one command for both. Each resource is applied in its own transaction (one
DuckLake snapshot) and advances the org's cursor, so an interrupted sync resumes
cleanly. Run `sync-all` on a schedule (cron, a systemd timer, a Kubernetes
CronJob, GitHub Actions) to keep the lake current. Lower `--batch-size` (default
100000) on memory-constrained machines.

The spill for large operations, and each resource's download/conversion, goes to
`$TMPDIR` — point that at a roomy volume if your default temp dir is small. Set
`PODLAKE_MEMORY_LIMIT` (e.g. `10GB` on a 16GB box) to cap DuckDB's buffer pool so
it spills to disk instead of growing. Note this bounds the *buffer pool* only;
the delete backlog described next is separate and is not covered by it.

### The delete backlog

This is the one operational thing worth understanding, because it will break
writes long before it fills a disk.

DuckLake is merge-on-read: a `DELETE` doesn't rewrite data files, it records the
removed rows as tombstones alongside them. Every delta upserts — deleting each
incoming record before inserting its new version — so a routine sync steadily
accumulates tombstones. In podlake's tall schema one changed record tombstones
~40 rows, one per subfield, so they build fast.

They aren't just dead weight on disk. **Every `DELETE` loads the full delete
history of each data file it opens and holds it in memory for the duration of
the statement** — memory that DuckDB's `memory_limit` does not bound. So the
backlog, more than the number of rows being deleted, is what decides whether a
delta fits in RAM. Left to grow, deltas get slower, then memory-hungry, then
fail outright: on a 32GB machine a 389k-record delta was fine against a 105M-row
backlog, while a 1.1M-record one was OOM-killed against 153M.

Compacting small files does **not** clear it — file merging skips files that are
already large, which is where the backlog lives. Only rewriting those files does,
which is why `compact` does it by default:

```
$ uvx podlake compact                          # reclaim disk + apply deletes
$ uvx podlake compact --delete-threshold 0.02  # rewrite files ≥2% deleted (slower, reclaims more)
$ uvx podlake compact --no-rewrite-deletes     # quick disk-only pass
$ uvx podlake compact --dry-run                # preview without changing anything
```

Every run reports the backlog, which is the number to watch:

```
pending deletes: 358 files / 557,856,506 rows → 99 files / 105,293,224 rows
```

`--delete-threshold` sets how dead a file must be before it's rewritten. Higher
is cheaper and reclaims less; each threshold has a floor it can't get below, so
lower it when a run stops making progress. Reclaiming the easy majority is fast
— on a lake with a 1.5B-row backlog, 93% of it cleared in about 8 minutes — but
cost per row rises steeply as the remainder thins out.

**Sync manages this for you.** Before applying each resource it checks the
backlog and clears it if needed, so a long delta chain — a first-time load, or
months of catch-up — doesn't grow its own memory use until it dies partway
through. Large resources are held to a stricter limit than small ones, since
they're what actually run out of memory. Tune with `--max-pending-deletes`
(default 100M rows; raise it if you have more RAM, `0` disables), and consider
running the sync under a memory cap so a bad estimate kills only the sync:

```
$ systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 \
    uvx podlake sync-all
```

**Publish** shares a local lake read-only by syncing its Parquet and catalog to
S3; consumers then attach over `s3://` with no database to reach. It's
incremental (skips files already uploaded), and additive — it never deletes from
the bucket, so republishing can't pull a file out from under a running query:

```
$ uvx podlake publish s3://your-bucket/pod   # or set PODLAKE_PUBLISH_URL
```

So the routine cycle is:

```
$ uvx podlake sync-all                        # apply new resources
$ uvx podlake compact                         # reclaim disk, apply deletes
$ uvx podlake publish s3://your-bucket/pod    # share it
```

Compacting before publishing also means consumers get files with the deletes
already applied, rather than paying to merge tombstones on every query.

Two lesser-used commands round things out: `fetch` downloads and converts an
org's dumps to Parquet **without** loading a lake (for inspection), and `load`
ingests such a records + meta Parquet pair directly.

## Query the lake

For a quick check, query through podlake (it connects read-only):

```
$ uvx podlake query "SELECT org, count(*) FROM record_meta GROUP BY org"
```

Analysts usually attach directly with DuckDB, **read-only** so the connection
can never modify the lake:

```sql
-- a published lake in a bucket (what most consumers use)
INSTALL ducklake; INSTALL httpfs;
ATTACH 'ducklake:s3://your-bucket/pod/podlake.ducklake' AS podlake
  (DATA_PATH 's3://your-bucket/pod/lake-data/', READ_ONLY, OVERRIDE_DATA_PATH true);
USE podlake;

-- a local file-catalog lake
INSTALL ducklake;
ATTACH 'ducklake:podlake.ducklake' AS podlake (DATA_PATH './lake-data/', READ_ONLY);
USE podlake;
```

`OVERRIDE_DATA_PATH true` re-roots the published catalog at the bucket. A public
bucket needs no credentials; for a private one, consumers supply read-only AWS
credentials via `CREATE SECRET (TYPE s3, ...)`. Thanks to snapshot isolation the
maintainer can republish while analysts keep querying, and a reader can pin a
version with `FROM records AT (VERSION => N)`. See the schema section above for
query patterns.

## Develop

```
$ uv run pytest
```

Tests run entirely locally (no network or `PODBUCKET_POD_TOKEN` needed):
ResourceSync manifest parsing with fixtures, MARCXML conversion with small
in-test dumps, and the lake/publish paths against a temporary file-profile lake
(S3 is mocked with moto).

[POD]: https://pod.stanford.edu/
[ResourceSync]: https://www.openarchives.org/rs/toc
[uv]: https://docs.astral.sh/uv/
[goldrush]: https://github.com/co-alliance/coalliance-matchkey
[DuckLake]: https://ducklake.select/
[mrrc]: https://github.com/dchud/mrrc/blob/main/docs/history/format-research/EVALUATION_PARQUET.md
