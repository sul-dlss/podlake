import logging
from datetime import date
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

    col_defs = ", ".join(f'"{name}" VARCHAR' for name in columns)
    con.execute(f'CREATE TABLE {RECORDS_TABLE} ("org" VARCHAR, {col_defs})')
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
        select_cols = ", ".join(f'"{name}"' for name in columns)
        con.execute(
            f"INSERT INTO {RECORDS_TABLE} "
            f"SELECT ? AS org, {select_cols} FROM read_parquet(?)",
            [org, str(parquet_path)],
        )
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
    Create the `harvest_state` table if needed. It records the last date each
    org was harvested through, so the next update can pass it as the OAI `from`.
    """
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} (org VARCHAR, last_harvest DATE)"
    )


def get_last_harvest(con: duckdb.DuckDBPyConnection, org: str) -> date | None:
    """
    Return the date this org was last harvested through, or None if it has never
    been harvested (in which case an update should do a full harvest).
    """
    ensure_state_table(con)
    row = con.execute(
        f"SELECT last_harvest FROM {STATE_TABLE} WHERE org = ?", [org]
    ).fetchone()
    return row[0] if row else None


def set_last_harvest(
    con: duckdb.DuckDBPyConnection, org: str, harvest_date: date
) -> None:
    ensure_state_table(con)
    con.execute(f"DELETE FROM {STATE_TABLE} WHERE org = ?", [org])
    con.execute(f"INSERT INTO {STATE_TABLE} VALUES (?, ?)", [org, harvest_date])


def apply_update(
    con: duckdb.DuckDBPyConnection,
    org: str,
    delta_parquet: Path,
    deleted_ids: list[str],
    harvest_date: date,
) -> tuple[int, int]:
    """
    Apply an incremental update for one org in a single transaction (one
    DuckLake snapshot): upsert the changed records in `delta_parquet` keyed by
    pod_record_id, remove any `deleted_ids`, and record `harvest_date` as the
    org's new last-harvest date. Returns (changed_count, deleted_count).

    The delta Parquet has the same columns as a full harvest, so the `records`
    table is created from it if this is the first data for the lake.
    """
    columns = _parquet_columns(con, delta_parquet)
    ensure_schema(con, columns)
    ensure_state_table(con)

    con.execute("BEGIN TRANSACTION")
    try:
        # Replace changed records: delete the existing rows for the incoming
        # pod_record_ids, then insert the new versions.
        con.execute(
            f"DELETE FROM {RECORDS_TABLE} WHERE pod_record_id IN "
            "(SELECT pod_record_id FROM read_parquet(?))",
            [str(delta_parquet)],
        )
        select_cols = ", ".join(f'"{name}"' for name in columns)
        con.execute(
            f"INSERT INTO {RECORDS_TABLE} "
            f"SELECT ? AS org, {select_cols} FROM read_parquet(?)",
            [org, str(delta_parquet)],
        )
        changed = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(delta_parquet)]
        ).fetchone()
        changed_count = changed[0] if changed else 0

        deleted_count = 0
        if deleted_ids:
            placeholders = ", ".join("?" for _ in deleted_ids)
            con.execute(
                f"DELETE FROM {RECORDS_TABLE} WHERE pod_record_id IN ({placeholders})",
                deleted_ids,
            )
            deleted_count = len(deleted_ids)

        con.execute(f"DELETE FROM {STATE_TABLE} WHERE org = ?", [org])
        con.execute(f"INSERT INTO {STATE_TABLE} VALUES (?, ?)", [org, harvest_date])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    logger.info(
        "update org=%s: %s changed, %s deleted", org, changed_count, deleted_count
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


def _parquet_columns(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> list[str]:
    rel = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(parquet_path)])
    return [description[0] for description in rel.description]


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
