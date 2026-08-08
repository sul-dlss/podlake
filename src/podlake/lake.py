import logging
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from podlake.config import LAKE_ALIAS, Config, get_config
from podlake.storage import Storage

logger = logging.getLogger(__name__)

RECORDS_TABLE = "records"
META_TABLE = "record_meta"
STATE_TABLE = "harvest_state"


def connect(
    read_only: bool = False, config: Config | None = None
) -> duckdb.DuckDBPyConnection:
    """
    Connect to the DuckLake catalog for the active profile and return an open
    DuckDB connection with the lake attached and selected.

    Pass read_only=True for consumer-style access that cannot modify the lake.
    """
    config = config or get_config()

    con = duckdb.connect()
    _load_extensions(con, config)
    _configure_storage(con, config)

    logger.info("attaching ducklake (%s, read_only=%s)", config.profile, read_only)
    con.execute(config.attach_sql(read_only=read_only))
    con.execute(f"USE {LAKE_ALIAS}")

    # Spill to the same temp location as downloads (honors $TMPDIR), and cap
    # DuckDB's memory if configured, so a big merge overflows to disk instead of
    # ballooning until the OOM killer steps in.
    spill = tempfile.gettempdir().replace("'", "''")
    con.execute(f"SET temp_directory = '{spill}'")
    if config.memory_limit:
        limit = config.memory_limit.replace("'", "''")
        con.execute(f"SET memory_limit = '{limit}'")

    # Show DuckDB's progress bar for long-running statements — notably the
    # delete+insert upsert that `apply_resource` runs after conversion — so a
    # busy apply step isn't mistaken for a hang. Only in an interactive
    # terminal, to keep non-interactive runs (cron/`sync-all`) quiet.
    if sys.stdout.isatty():
        con.execute("SET enable_progress_bar = true")

    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the tall `records` (EAV) table and the per-record `record_meta`
    table if they don't exist yet, each partitioned by org. The schema is fixed
    (see convert.RECORDS_SCHEMA / META_SCHEMA), so no column derivation is
    needed.
    """
    if _table_exists(con, RECORDS_TABLE):
        return

    con.execute(
        f"CREATE TABLE {RECORDS_TABLE} ("
        "org VARCHAR, pod_record_id VARCHAR, field_tag VARCHAR, field_seq INTEGER, "
        "ind1 VARCHAR, ind2 VARCHAR, subfield_code VARCHAR, subfield_seq INTEGER, "
        "value VARCHAR)"
    )
    con.execute(f"ALTER TABLE {RECORDS_TABLE} SET PARTITIONED BY (org)")

    con.execute(
        f"CREATE TABLE {META_TABLE} "
        "(org VARCHAR, pod_record_id VARCHAR, goldrush_key VARCHAR)"
    )
    con.execute(f"ALTER TABLE {META_TABLE} SET PARTITIONED BY (org)")
    logger.info("created %s + %s tables partitioned by org", RECORDS_TABLE, META_TABLE)


def load_pair(
    con: duckdb.DuckDBPyConnection,
    org: str,
    records_parquet: Path,
    meta_parquet: Path,
) -> int:
    """
    Load one organization's records + meta Parquet pair, replacing any existing
    rows for that org (idempotent). Returns the number of records loaded.
    """
    ensure_schema(con)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {RECORDS_TABLE} WHERE org = ?", [org])
        con.execute(f"DELETE FROM {META_TABLE} WHERE org = ?", [org])
        con.execute(
            f"INSERT INTO {RECORDS_TABLE} SELECT * FROM read_parquet(?)",
            [str(records_parquet)],
        )
        con.execute(
            f"INSERT INTO {META_TABLE} SELECT * FROM read_parquet(?)",
            [str(meta_parquet)],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    row = con.execute(
        f"SELECT count(*) FROM {META_TABLE} WHERE org = ?", [org]
    ).fetchone()
    loaded = row[0] if row else 0
    logger.info("loaded %s records for org=%s", loaded, org)
    return loaded


def ensure_state_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the `harvest_state` table if needed. It records the lastmod of the
    last ResourceSync resource processed per org (the sync cursor), so the next
    sync only processes newer resources.
    """
    # Stored as a naive UTC TIMESTAMP (not TIMESTAMPTZ, which would make DuckDB
    # require pytz on read); UTC is re-attached in get_cursor.
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} "
        "(org VARCHAR, last_modified TIMESTAMP)"
    )


def get_cursor(con: duckdb.DuckDBPyConnection, org: str) -> datetime | None:
    """
    Return the lastmod (UTC) of the last resource processed for this org, or
    None if it has never been synced (so the next sync starts from the full
    dump).
    """
    ensure_state_table(con)
    row = con.execute(
        f"SELECT last_modified FROM {STATE_TABLE} WHERE org = ?", [org]
    ).fetchone()
    if not row or row[0] is None:
        return None
    return row[0].replace(tzinfo=UTC)


def all_cursors(con: duckdb.DuckDBPyConnection) -> dict[str, datetime]:
    """
    Return {org: last processed lastmod (UTC)} from harvest_state, or {} if the
    lake has never been synced. Unlike get_cursor this never creates the table,
    so it is safe to call on a read-only connection (used by `podlake status`).
    """
    if not _table_exists(con, STATE_TABLE):
        return {}
    rows = con.execute(f"SELECT org, last_modified FROM {STATE_TABLE}").fetchall()
    return {org: ts.replace(tzinfo=UTC) for org, ts in rows if ts is not None}


def _set_cursor(
    con: duckdb.DuckDBPyConnection, org: str, last_modified: datetime
) -> None:
    con.execute(f"DELETE FROM {STATE_TABLE} WHERE org = ?", [org])
    con.execute(
        f"INSERT INTO {STATE_TABLE} VALUES (?, ?)", [org, _naive_utc(last_modified)]
    )


def _naive_utc(dt: datetime) -> datetime:
    """Normalize a datetime to a naive UTC value for the TIMESTAMP column."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC)
    return dt.replace(tzinfo=None)


def apply_resource(
    con: duckdb.DuckDBPyConnection,
    org: str,
    kind: str,
    payload: tuple[Path, Path] | list[str],
    last_modified: datetime,
) -> tuple[int, int]:
    """
    Apply one ResourceSync resource for an org in a single transaction (one
    DuckLake snapshot), then advance the org's sync cursor to `last_modified`:

    - "full"/"delta": `payload` is a (records_parquet, meta_parquet) pair; upsert
      the records by pod_record_id (delete existing ids from both tables, insert
      the new versions).
    - "deletes": `payload` is a list of pod_record_ids to delete from both tables.

    Returns (changed_count, deleted_count).
    """
    ensure_schema(con)
    ensure_state_table(con)

    changed_count = deleted_count = 0
    con.execute("BEGIN TRANSACTION")
    try:
        if kind in ("full", "delta"):
            assert isinstance(payload, tuple)
            records_pq, meta_pq = payload
            # incoming ids come from meta (one row per record)
            ids_subquery = "(SELECT pod_record_id FROM read_parquet(?))"
            # Scope both deletes to the org partition. Without the `org =`
            # predicate DuckLake can't prune, so the delete scans every org's
            # data to find this resource's ids — a full-table scan that is very
            # slow on a multi-org lake. Every id in the resource belongs to
            # `org`, so restricting to it is safe.
            con.execute(
                f"DELETE FROM {RECORDS_TABLE} "
                f"WHERE org = ? AND pod_record_id IN {ids_subquery}",
                [org, str(meta_pq)],
            )
            con.execute(
                f"DELETE FROM {META_TABLE} "
                f"WHERE org = ? AND pod_record_id IN {ids_subquery}",
                [org, str(meta_pq)],
            )
            con.execute(
                f"INSERT INTO {RECORDS_TABLE} SELECT * FROM read_parquet(?)",
                [str(records_pq)],
            )
            con.execute(
                f"INSERT INTO {META_TABLE} SELECT * FROM read_parquet(?)",
                [str(meta_pq)],
            )
            row = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(meta_pq)]
            ).fetchone()
            changed_count = row[0] if row else 0
        elif kind == "deletes":
            assert isinstance(payload, list)
            if payload:
                # Semi-join against the ids as a registered relation, like the
                # delta path reads ids from a Parquet subquery. A big deletes
                # file (200k+ ids) would otherwise be a giant literal
                # IN (?, ?, …) list — an enormous SQL string + bind. Register
                # the ids as a (zero-copy) Arrow table and reference it instead.
                con.register("delete_ids", pa.table({"pod_record_id": payload}))
                try:
                    id_subquery = "(SELECT pod_record_id FROM delete_ids)"
                    con.execute(
                        f"DELETE FROM {RECORDS_TABLE} "
                        f"WHERE org = ? AND pod_record_id IN {id_subquery}",
                        [org],
                    )
                    con.execute(
                        f"DELETE FROM {META_TABLE} "
                        f"WHERE org = ? AND pod_record_id IN {id_subquery}",
                        [org],
                    )
                finally:
                    con.unregister("delete_ids")
                deleted_count = len(payload)
        else:
            raise ValueError(f"unknown resource kind: {kind}")

        _set_cursor(con, org, last_modified)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    logger.info(
        "applied %s for org=%s: %s changed, %s deleted",
        kind,
        org,
        changed_count,
        deleted_count,
    )
    return changed_count, deleted_count


# --- chunked apply ----------------------------------------------------------
#
# `apply_resource` applies a whole resource in one transaction, which is fine
# for small resources but uses memory proportional to the resource size (the
# DuckLake insert's working memory is *not* bounded by DuckDB's memory_limit).
# For large full dumps / deltas the sync path instead streams the resource into
# `batch_size`-record Parquet parts (convert.dump_to_parquet_batches) and calls
# these one batch at a time, advancing the cursor only once all batches land.


def _timed(
    con: duckdb.DuckDBPyConnection, label: str, sql: str, params: list | None = None
) -> None:
    """
    Run one statement, logging "→ label" before and "← label (Ns)" after, so with
    `--verbose` the last unmatched "→" printed before a stall/OOM names the exact
    statement that's slow or memory-hungry.
    """
    logger.info("→ %s", label)
    start = time.perf_counter()
    if params is not None:
        con.execute(sql, params)
    else:
        con.execute(sql)
    logger.info("← %s (%.1fs)", label, time.perf_counter() - start)


def replace_partition(con: duckdb.DuckDBPyConnection, org: str) -> None:
    """
    Delete all of an org's rows (both tables) in one transaction. Used before
    reloading a full dump so records dropped upstream don't linger, and so the
    batched insert that follows can skip per-batch deletes entirely.
    """
    ensure_schema(con)
    con.execute("BEGIN TRANSACTION")
    try:
        _timed(
            con,
            f"{org} full: clear records",
            f"DELETE FROM {RECORDS_TABLE} WHERE org = ?",
            [org],
        )
        _timed(
            con,
            f"{org} full: clear record_meta",
            f"DELETE FROM {META_TABLE} WHERE org = ?",
            [org],
        )
        _timed(con, f"{org} full: clear commit", "COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def apply_insert_batch(
    con: duckdb.DuckDBPyConnection, org: str, records_pq: Path, meta_pq: Path
) -> int:
    """Insert one batch (no delete) in its own transaction. Returns records inserted."""
    ensure_schema(con)
    con.execute("BEGIN TRANSACTION")
    try:
        _timed(
            con,
            f"{org} insert: records",
            f"INSERT INTO {RECORDS_TABLE} SELECT * FROM read_parquet(?)",
            [str(records_pq)],
        )
        _timed(
            con,
            f"{org} insert: record_meta",
            f"INSERT INTO {META_TABLE} SELECT * FROM read_parquet(?)",
            [str(meta_pq)],
        )
        _timed(con, f"{org} insert: commit", "COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    row = con.execute("SELECT count(*) FROM read_parquet(?)", [str(meta_pq)]).fetchone()
    return row[0] if row else 0


def apply_upsert_batch(
    con: duckdb.DuckDBPyConnection, org: str, records_pq: Path, meta_pq: Path
) -> int:
    """
    Upsert one batch — delete the batch's ids (org-scoped) then insert — in its
    own transaction. Returns records upserted.
    """
    ensure_schema(con)
    ids = "(SELECT pod_record_id FROM read_parquet(?))"
    con.execute("BEGIN TRANSACTION")
    try:
        _timed(
            con,
            f"{org} upsert: delete records",
            f"DELETE FROM {RECORDS_TABLE} WHERE org = ? AND pod_record_id IN {ids}",
            [org, str(meta_pq)],
        )
        _timed(
            con,
            f"{org} upsert: delete record_meta",
            f"DELETE FROM {META_TABLE} WHERE org = ? AND pod_record_id IN {ids}",
            [org, str(meta_pq)],
        )
        _timed(
            con,
            f"{org} upsert: insert records",
            f"INSERT INTO {RECORDS_TABLE} SELECT * FROM read_parquet(?)",
            [str(records_pq)],
        )
        _timed(
            con,
            f"{org} upsert: insert record_meta",
            f"INSERT INTO {META_TABLE} SELECT * FROM read_parquet(?)",
            [str(meta_pq)],
        )
        _timed(con, f"{org} upsert: commit", "COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    row = con.execute("SELECT count(*) FROM read_parquet(?)", [str(meta_pq)]).fetchone()
    return row[0] if row else 0


def set_cursor(
    con: duckdb.DuckDBPyConnection, org: str, last_modified: datetime
) -> None:
    """
    Advance the org's sync cursor in its own transaction. Call once after all of
    a resource's batches have been applied, so an interrupted resource (cursor
    not yet advanced) is re-processed idempotently on the next sync.
    """
    ensure_state_table(con)
    con.execute("BEGIN TRANSACTION")
    try:
        _set_cursor(con, org, last_modified)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def publish(config: Config, dest_uri: str) -> tuple[str, str, int, int]:
    """
    Publish a file-catalog lake to an S3 bucket so read-only consumers can
    attach to it over s3://. Incrementally syncs the Parquet data under
    DATA_PATH (only new/changed files) and then uploads the catalog. Returns
    (catalog_key, data_prefix, uploaded, skipped).

    Data files are uploaded **before** the catalog so the published catalog only
    ever references files already present; DuckLake snapshot isolation keeps
    readers on the prior snapshot until the new catalog lands. Consumers attach
    with OVERRIDE_DATA_PATH because the catalog was written with a local
    DATA_PATH.
    """
    if not config.is_file_catalog:
        raise ValueError("publish requires a file-catalog lake, not a Postgres catalog")

    catalog_path = Path(config.catalog_uri)
    if not catalog_path.is_file():
        raise FileNotFoundError(f"catalog file not found: {catalog_path}")

    data_path = Path(config.data_path)
    data_prefix = data_path.name or "lake-data"
    catalog_key = catalog_path.name

    storage = Storage(dest_uri)
    # data first (immutable, additive; skips what's already uploaded)...
    uploaded, skipped = storage.sync_dir(data_path, data_prefix)
    # ...then the catalog last (it changes every publish).
    storage.upload_file(catalog_path, catalog_key)

    logger.info(
        "published to %s: %s data files uploaded, %s skipped (+ catalog)",
        dest_uri,
        uploaded,
        skipped,
    )
    return catalog_key, data_prefix, uploaded, skipped


def _snapshot_count(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute(
        f"SELECT count(*) FROM ducklake_snapshots('{LAKE_ALIAS}')"
    ).fetchone()
    return row[0] if row else 0


def compact(
    con: duckdb.DuckDBPyConnection, *, days: int = 0, dry_run: bool = False
) -> dict[str, int]:
    """
    Reclaim disk in the attached lake. DuckLake is merge-on-read, so superseded
    rows — from deltas/deletes and re-imported full dumps — pile up on disk until
    this runs. It expires snapshots older than `days` days (0 = all but the
    current), compacts small Parquet files, then deletes the data files no longer
    referenced by a live snapshot. With dry_run=True nothing changes and the
    counts report what *would* be reclaimed. Returns a stats dict.
    """
    dry = "true" if dry_run else "false"
    older_than = f"now() - INTERVAL '{int(days)} days'"

    def run(fn: str) -> int:
        # Materialize via CTAS so the side-effecting table function runs fully: a
        # bare count() can be optimized away, and fetching the functions'
        # TIMESTAMPTZ columns into Python would require pytz. The temp table keeps
        # the timestamps inside DuckDB.
        con.execute(f"CREATE OR REPLACE TEMP TABLE _compact AS SELECT * FROM {fn}")
        row = con.execute("SELECT count(*) FROM _compact").fetchone()
        return row[0] if row else 0

    before = _snapshot_count(con)
    expired = run(
        f"ducklake_expire_snapshots('{LAKE_ALIAS}', "
        f"dry_run => {dry}, older_than => {older_than})"
    )
    # merge has no dry-run mode, so only compact for real runs
    merged = 0 if dry_run else run(f"ducklake_merge_adjacent_files('{LAKE_ALIAS}')")
    cleaned = run(
        f"ducklake_cleanup_old_files('{LAKE_ALIAS}', "
        f"dry_run => {dry}, cleanup_all => true)"
    )
    orphaned = run(
        f"ducklake_delete_orphaned_files('{LAKE_ALIAS}', "
        f"dry_run => {dry}, cleanup_all => true)"
    )
    con.execute("DROP TABLE IF EXISTS _compact")

    stats = {
        "snapshots_before": before,
        "snapshots_after": _snapshot_count(con),
        "expired": expired,
        "merged": merged,
        "cleaned": cleaned,
        "orphaned": orphaned,
    }
    logger.info("compact: %s", stats)
    return stats


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def _load_extensions(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    extensions = ["ducklake"]
    if config.is_postgres:
        extensions += ["postgres", "httpfs", "aws"]
    for ext in extensions:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")


def _configure_storage(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """
    With the postgres profile the DATA_PATH lives in S3, so register an S3
    secret that resolves credentials via DuckDB's credential_chain (standard
    AWS_* env vars, shared config, or an assumed role).
    """
    if config.is_postgres and config.data_path.startswith("s3://"):
        con.execute(
            "CREATE SECRET IF NOT EXISTS pod_s3 (TYPE s3, PROVIDER credential_chain)"
        )
