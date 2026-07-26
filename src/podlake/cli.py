import os
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import duckdb
import typer
from rich import print
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

from podlake import lake
from podlake.config import get_config
from podlake.convert import oai_to_parquet
from podlake.oai import get_set, list_sets

app = typer.Typer()

# tqdm defaults to multiprocessing.RLock, which creates a named OS semaphore.
# Using threading.RLock avoids that so os._exit() on Ctrl-C doesn't leave a
# leaked semaphore that triggers a resource_tracker warning at shutdown.
tqdm.set_lock(threading.RLock())


@app.command()
def config():
    """
    Show the resolved configuration for the active profile (set with the
    PODLAKE_ENV environment variable) and verify that the DuckLake catalog can
    be attached.
    """
    cfg = get_config()

    print(f"[bold]podlake configuration[/bold] ({cfg.env})")
    for key, value in cfg.describe().items():
        print(f"  {key} = {value}")

    try:
        con = lake.connect(read_only=True, config=cfg)
        has_table = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_name = '{lake.RECORDS_TABLE}'"
        ).fetchone()
        if has_table and has_table[0]:
            row = con.execute(f"SELECT count(*) FROM {lake.RECORDS_TABLE}").fetchone()
            count = row[0] if row else 0
            print(f"[green]✓ connected to the DuckLake ({count} records)[/green]")
        else:
            print(
                "[green]✓ connected to the DuckLake[/green] "
                "[yellow](no records loaded yet — run `podlake load`)[/yellow]"
            )
        con.close()
    except duckdb.Error as e:
        # A missing catalog just means the lake hasn't been built yet, which is
        # a normal state, not a configuration error.
        print(
            "[yellow]! could not attach to the DuckLake — it may not be built "
            f"yet (run `podlake load`): {e}[/yellow]"
        )


@app.command()
def sets():
    """
    Output the sets available.
    """
    get_config()
    for s in list_sets():
        print(f"- [bold]{s.contributor}[/bold] id={s.setSpec}")  # ty: ignore[unresolved-attribute]


@app.command()
def convert(
    org_name: Annotated[str, typer.Argument(help="Organization name")],
    output_path: Annotated[
        Path, typer.Argument(help="Path to write Parquet file", dir_okay=False)
    ],
    limit: Annotated[int | None, typer.Option(help="Limit number of records")] = None,
):
    """
    Harvest records for the given organization name: e.g. "stanford" and write
    them to the supplied parquet file path.
    """
    get_config()

    set_ = get_set(org_name.lower())
    if set_ is None:
        typer.echo(f"Can't find POD set for {org_name}", err=True)
        raise typer.Exit(code=1)

    with tqdm(
        desc=f"harvesting {org_name}", unit=" records", smoothing=0.01
    ) as progress:
        oai_to_parquet(
            org_name,
            output_path,
            limit,
            on_record=lambda _: progress.update(1),
        )


@app.command()
def convert_all(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory to write Parquet files", dir_okay=True, file_okay=False
        ),
    ],
    workers: Annotated[
        int, typer.Option(help="Number of worker processes to use in parallel")
    ] = 1,
):
    """
    Harvest all records and write them organization specific parquet files in
    the supplied directory. Use --workers to control the number of concurrent
    workers to use.
    """
    get_config()
    if output_dir.is_dir() is False:
        output_dir.mkdir(parents=True)

    sets = list_sets()
    set_args = [(s.contributor, output_dir) for s in sets]  # ty: ignore[unresolved-attribute]

    try:
        thread_map(_convert, set_args, max_workers=workers, desc="converting sets")
    except KeyboardInterrupt:
        typer.echo("\ninterrupted", err=True)
        # os._exit bypasses Python's atexit handlers, including the one in
        # concurrent.futures that joins all threads. Without this, a second
        # KeyboardInterrupt raised during that join produces a traceback.
        os._exit(1)


_thread_local = threading.local()
_next_position = 0
_position_lock = threading.Lock()


def _thread_position():
    if not hasattr(_thread_local, "position"):
        global _next_position
        with _position_lock:
            _next_position += 1
            _thread_local.position = _next_position
    return _thread_local.position


def _convert(set_args):
    set_name, output_dir = set_args
    parquet_path = output_dir / f"{set_name}.parquet"
    with tqdm(
        desc=set_name, unit=" records", smoothing=0.01, position=_thread_position()
    ) as progress:
        oai_to_parquet(set_name, parquet_path, on_record=lambda _: progress.update(1))


@app.command()
def load(
    path: Annotated[
        Path,
        typer.Argument(
            help="A Parquet file, or a directory of per-org Parquet files",
            exists=True,
        ),
    ],
    org: Annotated[
        str | None,
        typer.Option(
            help="Organization name for a single Parquet file "
            "(defaults to the file name without extension)"
        ),
    ] = None,
):
    """
    Build or refresh the DuckLake by loading Parquet produced by `convert` or
    `convert-all` into the unified `records` table, partitioned by org. Loading
    an org that already exists replaces its rows, so this is safe to re-run.
    """
    get_config()

    if path.is_dir():
        parquet_files = sorted(path.glob("*.parquet"))
        if not parquet_files:
            typer.echo(f"No .parquet files found in {path}", err=True)
            raise typer.Exit(code=1)
        jobs = [(p, p.stem) for p in parquet_files]
    else:
        jobs = [(path, org or path.stem)]

    con = lake.connect(read_only=False)
    try:
        for parquet_path, org_name in jobs:
            count = lake.load_parquet(con, parquet_path, org_name)
            print(f"loaded [bold]{count}[/bold] records for {org_name}")
    finally:
        con.close()


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="A read-only SQL query to run")],
):
    """
    Run a read-only SQL query against the DuckLake and print the results. Useful
    for quick checks; the unified table is named `records`.
    """
    get_config()

    con = lake.connect(read_only=True)
    try:
        con.sql(sql).show()
    finally:
        con.close()


@app.command()
def publish(
    dest: Annotated[
        str | None,
        typer.Argument(
            help="s3://bucket/prefix to publish to "
            "(defaults to the PODLAKE_PUBLISH_URL environment variable)"
        ),
    ] = None,
):
    """
    Publish a file-catalog lake to an S3 bucket so read-only consumers can
    attach to it over s3://. Uploads the catalog file and all Parquet data.
    """
    cfg = get_config()

    target = dest or cfg.publish_url
    if not target:
        typer.echo(
            "No publish destination: pass an s3:// URL or set PODLAKE_PUBLISH_URL",
            err=True,
        )
        raise typer.Exit(code=1)
    if not cfg.is_file_catalog:
        typer.echo(
            "publish requires a file-catalog lake, not a Postgres catalog", err=True
        )
        raise typer.Exit(code=1)

    catalog_key, data_prefix, count = lake.publish(cfg, target)
    print(f"published [bold]{count}[/bold] files to {target}")

    base = target.rstrip("/")
    print("\nConsumers can attach read-only with:")
    print("  INSTALL ducklake; INSTALL httpfs;")
    print(f"  ATTACH 'ducklake:{base}/{catalog_key}' AS podlake")
    print(
        f"    (DATA_PATH '{base}/{data_prefix}/', READ_ONLY, OVERRIDE_DATA_PATH true);"
    )


@app.command()
def update(
    org_name: Annotated[str, typer.Argument(help="Organization name")],
    since: Annotated[
        str | None,
        typer.Option(
            help="Override the from date (YYYY-MM-DD); defaults to the org's "
            "last-harvest date, or a full harvest if it has never been harvested"
        ),
    ] = None,
):
    """
    Incrementally update one organization in the DuckLake: harvest records
    changed since its last harvest, upsert them, apply any deletions POD
    reports, and record today as the new last-harvest date.
    """
    get_config()

    set_ = get_set(org_name.lower())
    if set_ is None:
        typer.echo(f"Can't find POD set for {org_name}", err=True)
        raise typer.Exit(code=1)

    con = lake.connect(read_only=False)
    try:
        changed, deleted = _update_org(con, org_name, since=since)
        print(f"[bold]{org_name}[/bold]: {changed} changed, {deleted} deleted")
    finally:
        con.close()


@app.command()
def update_all():
    """
    Incrementally update every organization in the DuckLake, one at a time.
    Each org is its own transaction (DuckLake snapshot), so a failure part way
    through leaves already-updated orgs committed.
    """
    get_config()

    con = lake.connect(read_only=False)
    try:
        for s in list_sets():
            org_name = s.contributor  # ty: ignore[unresolved-attribute]
            changed, deleted = _update_org(con, org_name)
            print(f"[bold]{org_name}[/bold]: {changed} changed, {deleted} deleted")
    finally:
        con.close()


def _update_org(
    con: duckdb.DuckDBPyConnection, org_name: str, since: str | None = None
) -> tuple[int, int]:
    if since is not None:
        from_ = since
    else:
        last = lake.get_last_harvest(con, org_name)
        from_ = last.isoformat() if last else None

    deleted: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        delta_path = Path(tmp) / f"{org_name}.parquet"
        with tqdm(
            desc=f"updating {org_name}", unit=" records", smoothing=0.01
        ) as progress:
            oai_to_parquet(
                org_name,
                delta_path,
                from_=from_,
                deleted=deleted,
                on_record=lambda _: progress.update(1),
            )
        # OAI datestamps are UTC, so record the harvest date in UTC too.
        harvest_date = datetime.now(UTC).date()
        return lake.apply_update(con, org_name, delta_path, deleted, harvest_date)


if __name__ == "__main__":
    app()
