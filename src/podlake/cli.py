import os
import threading
from pathlib import Path

import typer
from rich import print
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from typing_extensions import Annotated

from podlake.config import get_config
from podlake.oai import list_sets, get_set
from podlake.convert import oai_to_parquet

app = typer.Typer()

# tqdm defaults to multiprocessing.RLock, which creates a named OS semaphore.
# Using threading.RLock avoids that so os._exit() on Ctrl-C doesn't leave a
# leaked semaphore that triggers a resource_tracker warning at shutdown.
tqdm.set_lock(threading.RLock())


@app.command()
def config():
    print("Configuring")


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


if __name__ == "__main__":
    app()
