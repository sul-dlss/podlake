import logging
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from podlake.config import LAKE_ALIAS, Config, get_config
from podlake.storage import Storage

logger = logging.getLogger(__name__)

RECORDS_TABLE = "records"
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

    logger.info("attaching ducklake (%s, read_only=%s)", config.env, read_only)
    con.execute(config.attach_sql(read_only=read_only))
    con.execute(f"USE {LAKE_ALIAS}")

    return con


def ensure_schema(con: duckdb.DuckDBPyConnection, columns: list[str]) -> None:
    """
    Create the unified `records` table (partitioned by org) if it does not yet
    exist. `columns` is the ordered list of Parquet column names produced by the
    conversion step; every one is stored as VARCHAR, matching marctable output.
    An `org` column is prepended as the partition key.
    """
    existing = con.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_name = '{RECORDS_TABLE}'"
    ).fetchall()
    if existing:
        return

    col_defs = ", ".join(f"{_quote_ident(name)} VARCHAR" for name in columns)
    con.execute(
        f"CREATE TABLE {RECORDS_TABLE} ({_quote_ident('org')} VARCHAR, {col_defs})"
    )
    con.execute(f"ALTER TABLE {RECORDS_TABLE} SET PARTITIONED BY (org)")
    logger.info("created %s table partitioned by org", RECORDS_TABLE)


def load_parquet(con: duckdb.DuckDBPyConnection, parquet_path: Path, org: str) -> int:
    """
    Load a single organization's Parquet file into the `records` table,
    replacing any existing rows for that org. Returns the number of rows loaded.

    The Parquet files produced by the conversion step have no `org` column, so
    it is added here from the supplied org name. This is idempotent: loading the
    same org again replaces its rows rather than duplicating them.
    """
    columns = _parquet_columns(con, parquet_path)
    ensure_schema(con, columns)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {RECORDS_TABLE} WHERE org = ?", [org])
        _insert_parquet(con, org, parquet_path, columns)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    count = con.execute(
        f"SELECT count(*) FROM {RECORDS_TABLE} WHERE org = ?", [org]
    ).fetchone()
    loaded = count[0] if count else 0
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


def set_cursor(
    con: duckdb.DuckDBPyConnection, org: str, last_modified: datetime
) -> None:
    ensure_state_table(con)
    con.execute("BEGIN TRANSACTION")
    try:
        _set_cursor(con, org, last_modified)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


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
    payload: Path | list[str],
    last_modified: datetime,
) -> tuple[int, int]:
    """
    Apply one ResourceSync resource for an org in a single transaction (one
    DuckLake snapshot), then advance the org's sync cursor to `last_modified`:

    - "full"/"delta": `payload` is a Parquet path; upsert its records by
      pod_record_id (delete existing ids, insert the new versions).
    - "deletes": `payload` is a list of pod_record_ids to delete.

    Returns (changed_count, deleted_count).
    """
    ensure_state_table(con)

    changed_count = deleted_count = 0
    con.execute("BEGIN TRANSACTION")
    try:
        if kind in ("full", "delta"):
            assert isinstance(payload, Path)
            columns = _parquet_columns(con, payload)
            ensure_schema(con, columns)
            con.execute(
                f"DELETE FROM {RECORDS_TABLE} WHERE pod_record_id IN "
                "(SELECT pod_record_id FROM read_parquet(?))",
                [str(payload)],
            )
            _insert_parquet(con, org, payload, columns)
            row = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(payload)]
            ).fetchone()
            changed_count = row[0] if row else 0
        elif kind == "deletes":
            assert isinstance(payload, list)
            # A deletes resource can precede any data (nothing to delete yet).
            if payload and _table_exists(con, RECORDS_TABLE):
                placeholders = ", ".join("?" for _ in payload)
                con.execute(
                    f"DELETE FROM {RECORDS_TABLE} "
                    f"WHERE pod_record_id IN ({placeholders})",
                    payload,
                )
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


def publish(config: Config, dest_uri: str) -> tuple[str, str, int]:
    """
    Publish a file-catalog lake to an S3 bucket so read-only consumers can
    attach to it over s3://. Uploads the catalog file and every Parquet data
    file under DATA_PATH. Returns (catalog_key, data_prefix, files_uploaded).

    Consumers attach with the catalog and data at their published locations,
    passing OVERRIDE_DATA_PATH because the catalog was written with a local
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
    storage.upload_file(catalog_path, catalog_key)
    data_files = storage.sync_dir(data_path, data_prefix)

    logger.info("published %s + %s data files to %s", catalog_key, data_files, dest_uri)
    return catalog_key, data_prefix, data_files + 1


def _insert_parquet(
    con: duckdb.DuckDBPyConnection,
    org: str,
    parquet_path: Path,
    columns: list[str],
) -> None:
    """
    Insert every row of a Parquet file into `records`, prepending the org
    partition column. Shared by the full-load and incremental-update paths.
    """
    select_cols = ", ".join(_quote_ident(name) for name in columns)
    con.execute(
        f"INSERT INTO {RECORDS_TABLE} "
        f"SELECT ? AS org, {select_cols} FROM read_parquet(?)",
        [org, str(parquet_path)],
    )


def _quote_ident(name: str) -> str:
    # Column names come from the Parquet schema (marctable's fixed field/subfield
    # set), so they are trusted, but quote-escape defensively since identifiers
    # can't be passed as bind parameters.
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _parquet_columns(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> list[str]:
    rel = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(parquet_path)])
    return [description[0] for description in rel.description]


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def _load_extensions(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    extensions = ["ducklake"]
    if config.is_production:
        extensions += ["postgres", "httpfs", "aws"]
    for ext in extensions:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")


def _configure_storage(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """
    In production the DATA_PATH lives in S3, so register an S3 secret that
    resolves credentials via DuckDB's credential_chain (standard AWS_* env
    vars, shared config, or an assumed role).
    """
    if config.is_production and config.data_path.startswith("s3://"):
        con.execute(
            "CREATE SECRET IF NOT EXISTS pod_s3 (TYPE s3, PROVIDER credential_chain)"
        )
