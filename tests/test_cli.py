import datetime
import gzip

import duckdb
from typer.testing import CliRunner

from podlake import lake, resourcesync
from podlake.cli import app
from podlake.config import get_config
from podlake.resourcesync import Resource

runner = CliRunner()

COLLECTION = (
    '<collection xmlns="http://www.loc.gov/MARC21/slim">'
    "<record><leader>00000nam a2200000 a 4500</leader>"
    '<controlfield tag="001">a1</controlfield>'
    '<datafield tag="245" ind1="0" ind2="0">'
    '<subfield code="a">One</subfield></datafield></record>'
    "<record><leader>00000nam a2200000 a 4500</leader>"
    '<controlfield tag="001">a2</controlfield>'
    '<datafield tag="245" ind1="0" ind2="0">'
    '<subfield code="a">Two</subfield></datafield></record>'
    "</collection>"
)
BASE = "https://pod.stanford.edu/file"


def _resources():
    ts = datetime.datetime(2026, 2, 11, tzinfo=datetime.UTC)
    return [
        Resource(
            f"{BASE}/1/brown-2026-02-11-full-marcxml.xml.gz",
            "application/gzip",
            1,
            "",
            ts,
            "full",
        ),
        Resource(
            f"{BASE}/2/brown-2026-02-12-delta-marcxml.xml.gz",
            "application/gzip",
            1,
            "",
            ts,
            "delta",
        ),
        Resource(
            f"{BASE}/3/brown-2026-02-12-delta-deletes.del.txt",
            "text/plain",
            1,
            "",
            ts,
            "deletes",
        ),
    ]


def _fake_download(url, path, fixity=None, desc=None):
    if url.endswith(".del.txt"):
        path.write_text("a2\n")  # delete record a2
    else:
        with gzip.open(path, "wb") as fh:
            fh.write(COLLECTION.encode())
    return path


def _patch(monkeypatch):
    monkeypatch.setattr(
        resourcesync, "get_streams", lambda name=None: {"brown": f"{BASE}/rl"}
    )
    monkeypatch.setattr(resourcesync, "get_resources", lambda url: _resources())
    monkeypatch.setattr(resourcesync, "download", _fake_download)


def test_fetch_writes_records_and_meta(tmp_path, monkeypatch):
    _patch(monkeypatch)
    out = tmp_path / "out"

    result = runner.invoke(app, ["fetch", "brown", str(out)])
    assert result.exit_code == 0, result.output

    assert (out / "brown-2026-02-11-full-marcxml.records.parquet").is_file()
    assert (out / "brown-2026-02-11-full-marcxml.meta.parquet").is_file()
    assert (out / "brown-2026-02-12-delta-marcxml.records.parquet").is_file()
    assert (out / "brown-2026-02-12-delta-deletes.del.txt").is_file()

    con = duckdb.connect()
    meta = out / "brown-2026-02-11-full-marcxml.meta.parquet"
    ids = con.execute(
        f"SELECT pod_record_id FROM read_parquet('{meta}') ORDER BY pod_record_id"
    ).fetchall()
    assert [i[0] for i in ids] == ["brown:a1", "brown:a2"]

    # fetch never builds a lake
    assert not list(tmp_path.rglob("*.ducklake"))


def test_fetch_full_only(tmp_path, monkeypatch):
    _patch(monkeypatch)
    out = tmp_path / "out"

    result = runner.invoke(app, ["fetch", "brown", str(out), "--full-only"])
    assert result.exit_code == 0, result.output

    names = sorted(p.name for p in out.iterdir())
    assert names == [
        "brown-2026-02-11-full-marcxml.meta.parquet",
        "brown-2026-02-11-full-marcxml.records.parquet",
    ]


def test_status_reports_pending_then_processed(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    # before any sync: lake not built, so everything is pending
    before = runner.invoke(app, ["status", "brown"])
    assert before.exit_code == 0, before.output
    assert "0 processed" in before.output
    assert "3 pending" in before.output
    assert "never synced" in before.output

    # after syncing all three resources, they all count as processed
    assert runner.invoke(app, ["sync", "brown"]).exit_code == 0
    after = runner.invoke(app, ["status", "brown"])
    assert after.exit_code == 0, after.output
    assert "3 processed" in after.output
    assert "0 pending" in after.output


def test_status_totals_and_percent_across_orgs(tmp_path, monkeypatch):
    monkeypatch.setattr(
        resourcesync,
        "get_streams",
        lambda name=None: {"brown": f"{BASE}/rl", "cornell": f"{BASE}/rl"},
    )
    monkeypatch.setattr(resourcesync, "get_resources", lambda url: _resources())
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0, result.output
    # 2 orgs x 3 resources, none synced -> total 6 pending, 0% done by size
    assert "total" in result.output
    assert "6 pending" in result.output
    assert "0% by size" in result.output


def test_status_errors_when_lake_exists_but_unreadable(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    # a catalog file that exists but isn't a valid DuckLake -> attach fails
    catalog = tmp_path / "podlake.ducklake"
    catalog.write_text("not a real ducklake catalog")
    monkeypatch.setenv("PODLAKE_CATALOG", str(catalog))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["status", "brown"])
    # errors out instead of silently reporting everything as "never synced"
    assert result.exit_code == 1
    assert "never synced" not in result.output


def test_sync_loads_into_lake(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["sync", "brown"])
    assert result.exit_code == 0, result.output

    con = lake.connect(read_only=True, config=get_config())
    # full loaded a1,a2; the deletes resource removed a2 -> only a1 remains
    ids = [
        r[0]
        for r in con.execute(
            "SELECT pod_record_id FROM record_meta ORDER BY pod_record_id"
        ).fetchall()
    ]
    assert ids == ["brown:a1"]
    con.close()
