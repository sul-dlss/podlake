import tempfile
from pathlib import Path
from typing import Annotated

import duckdb
import humanize
import typer
from rich import print
from tqdm import tqdm

from podlake import lake, resourcesync
from podlake.config import get_config
from podlake.convert import dump_to_parquet

app = typer.Typer()


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
                "[yellow](no records yet — run `podlake sync`)[/yellow]"
            )
        con.close()
    except duckdb.Error as e:
        # A missing catalog just means the lake hasn't been built yet, which is
        # a normal state, not a configuration error.
        print(
            "[yellow]! could not attach to the DuckLake — it may not be built "
            f"yet (run `podlake sync`): {e}[/yellow]"
        )


@app.command()
def streams(
    org_name: Annotated[
        str | None, typer.Argument(help="Limit to one organization")
    ] = None,
):
    """
    List POD organizations (ResourceSync streams) and how many resources each
    has (full dump + deltas + deletes), with total download size.
    """
    get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    for name, url in sorted(found.items()):
        resources = resourcesync.get_resources(url)
        total = sum(r.length for r in resources)
        print(
            f"- [bold]{name}[/bold]: {len(resources)} resources, "
            f"{humanize.naturalsize(total)}"
        )


@app.command()
def sync(
    org_name: Annotated[str, typer.Argument(help="Organization name")],
    batch_size: Annotated[
        int,
        typer.Option(
            help="Records buffered per Parquet row group. Lower it to reduce "
            "peak memory on constrained machines."
        ),
    ] = 100_000,
    limit: Annotated[
        int | None,
        typer.Option(help="Limit records per resource (useful for testing)"),
    ] = None,
):
    """
    Sync one organization from POD ResourceSync into the DuckLake. Processes
    every resource (full dump + deltas + deletes) newer than the org's cursor:
    the first run does a full initial load, later runs apply only new deltas.
    """
    get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    con = lake.connect(read_only=False)
    try:
        for name, url in found.items():
            changed, deleted, n = _sync_org(con, name, url, batch_size, limit)
            print(
                f"[bold]{name}[/bold]: {n} resources processed, "
                f"{changed} changed, {deleted} deleted"
            )
    finally:
        con.close()


@app.command()
def sync_all(
    batch_size: Annotated[
        int,
        typer.Option(
            help="Records buffered per Parquet row group. Lower it to reduce "
            "peak memory on constrained machines."
        ),
    ] = 100_000,
):
    """
    Sync every organization from POD ResourceSync into the DuckLake, one at a
    time. Each resource is its own transaction (DuckLake snapshot), so an
    interrupted run resumes cleanly from where it left off.
    """
    get_config()

    con = lake.connect(read_only=False)
    try:
        for name, url in sorted(resourcesync.get_streams().items()):
            changed, deleted, n = _sync_org(con, name, url, batch_size, None)
            print(
                f"[bold]{name}[/bold]: {n} resources processed, "
                f"{changed} changed, {deleted} deleted"
            )
    finally:
        con.close()


def _sync_org(
    con: duckdb.DuckDBPyConnection,
    org: str,
    resourcelist_url: str,
    batch_size: int,
    limit: int | None,
) -> tuple[int, int, int]:
    cursor = lake.get_cursor(con, org)
    resources = resourcesync.get_resources(resourcelist_url)
    pending = [r for r in resources if cursor is None or r.lastmod > cursor]

    total_changed = total_deleted = 0
    for resource in pending:
        with tempfile.TemporaryDirectory() as tmp:
            if resource.kind == "deletes":
                del_path = Path(tmp) / "deletes.txt"
                resourcesync.download(resource.url, del_path, fixity=resource.fixity)
                ids = [f"{org}:{rid}" for rid in resourcesync.read_delete_ids(del_path)]
                _, deleted = lake.apply_resource(
                    con, org, "deletes", ids, resource.lastmod
                )
                total_deleted += deleted
            else:
                suffix = ".xml.gz" if resource.url.endswith(".gz") else ".xml"
                dl_path = Path(tmp) / f"resource{suffix}"
                resourcesync.download(resource.url, dl_path, fixity=resource.fixity)
                records_pq = Path(tmp) / "records.parquet"
                meta_pq = Path(tmp) / "meta.parquet"
                with tqdm(
                    desc=f"{org} {resource.kind}", unit=" records", smoothing=0.01
                ) as progress:
                    dump_to_parquet(
                        org,
                        dl_path,
                        records_pq,
                        meta_pq,
                        batch_size=batch_size,
                        on_record=lambda _: progress.update(1),
                        limit=limit,
                    )
                changed, _ = lake.apply_resource(
                    con, org, resource.kind, (records_pq, meta_pq), resource.lastmod
                )
                total_changed += changed
    return total_changed, total_deleted, len(pending)


@app.command()
def fetch(
    org_name: Annotated[str, typer.Argument(help="Organization name")],
    output_dir: Annotated[
        Path,
        typer.Argument(help="Directory to write Parquet files", file_okay=False),
    ],
    full_only: Annotated[
        bool,
        typer.Option(help="Fetch only the base full dump (skip deltas and deletes)"),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(
            help="Records buffered per Parquet row group. Lower it to reduce "
            "peak memory on constrained machines."
        ),
    ] = 100_000,
    limit: Annotated[
        int | None,
        typer.Option(help="Limit records per resource (useful for testing)"),
    ] = None,
):
    """
    Download and convert an organization's ResourceSync dumps to Parquet files
    on disk, WITHOUT loading them into the DuckLake. Useful for inspecting the
    raw converted data (schema, column sparsity, sizes) without paying for a
    load. Use --full-only for just the base full dump.
    """
    get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    output_dir.mkdir(parents=True, exist_ok=True)
    for name, url in found.items():
        written, total = _fetch_org(name, url, output_dir, full_only, batch_size, limit)
        print(
            f"[bold]{name}[/bold]: wrote {written} files "
            f"({humanize.naturalsize(total)}) to {output_dir}"
        )


def _fetch_org(
    org: str,
    resourcelist_url: str,
    output_dir: Path,
    full_only: bool,
    batch_size: int,
    limit: int | None,
) -> tuple[int, int]:
    resources = resourcesync.get_resources(resourcelist_url)
    if full_only:
        resources = [r for r in resources if r.kind == "full"]

    written = total_bytes = 0
    for resource in resources:
        name = resource.url.rstrip("/").split("/")[-1]
        if resource.kind == "deletes":
            dest = output_dir / name
            resourcesync.download(resource.url, dest, fixity=resource.fixity)
            total_bytes += dest.stat().st_size
            written += 1
            continue

        stem = name.removesuffix(".gz").removesuffix(".xml")
        records_out = output_dir / f"{stem}.records.parquet"
        meta_out = output_dir / f"{stem}.meta.parquet"
        with tempfile.TemporaryDirectory() as tmp:
            suffix = ".xml.gz" if resource.url.endswith(".gz") else ".xml"
            dl_path = Path(tmp) / f"resource{suffix}"
            resourcesync.download(resource.url, dl_path, fixity=resource.fixity)
            with tqdm(
                desc=f"{org} {resource.kind}", unit=" records", smoothing=0.01
            ) as progress:
                dump_to_parquet(
                    org,
                    dl_path,
                    records_out,
                    meta_out,
                    batch_size=batch_size,
                    on_record=lambda _: progress.update(1),
                    limit=limit,
                )
        total_bytes += records_out.stat().st_size + meta_out.stat().st_size
        written += 2

    return written, total_bytes


@app.command()
def load(
    records_parquet: Annotated[
        Path,
        typer.Argument(
            help="A `*.records.parquet` file produced by `fetch`", exists=True
        ),
    ],
    meta_parquet: Annotated[
        Path | None,
        typer.Option(
            help="The matching meta Parquet (defaults to the sibling `*.meta.parquet`)"
        ),
    ] = None,
    org: Annotated[
        str | None,
        typer.Option(help="Organization name (defaults to the filename prefix)"),
    ] = None,
):
    """
    Load a records + meta Parquet pair produced by `fetch` into the DuckLake,
    replacing that org's rows (whole-org replace, so safe to re-run). Most
    ingestion should use `sync`; this is a manual escape hatch for a fetched
    full dump.
    """
    get_config()

    name = records_parquet.name
    if ".records.parquet" not in name:
        typer.echo("expected a `*.records.parquet` file", err=True)
        raise typer.Exit(code=1)
    meta = meta_parquet or records_parquet.with_name(
        name.replace(".records.parquet", ".meta.parquet")
    )
    if not meta.is_file():
        typer.echo(f"meta Parquet not found: {meta}", err=True)
        raise typer.Exit(code=1)
    org_name = org or name.split("-")[0]

    con = lake.connect(read_only=False)
    try:
        count = lake.load_pair(con, org_name, records_parquet, meta)
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


if __name__ == "__main__":
    app()
