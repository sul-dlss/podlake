import datetime
import gzip

import pandas
from typer.testing import CliRunner

from podlake import resourcesync
from podlake.cli import app
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


def _fake_download(url, path, fixity=None):
    if url.endswith(".del.txt"):
        path.write_text("991\n")
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


def test_fetch_writes_parquet_without_lake(tmp_path, monkeypatch):
    _patch(monkeypatch)
    out = tmp_path / "out"

    result = runner.invoke(app, ["fetch", "brown", str(out)])
    assert result.exit_code == 0, result.output

    # full + delta -> parquet; deletes copied as-is
    assert (out / "brown-2026-02-11-full-marcxml.parquet").is_file()
    assert (out / "brown-2026-02-12-delta-marcxml.parquet").is_file()
    assert (out / "brown-2026-02-12-delta-deletes.del.txt").is_file()

    df = pandas.read_parquet(out / "brown-2026-02-11-full-marcxml.parquet")
    assert df["pod_record_id"].tolist() == ["brown:a1", "brown:a2"]

    # fetch never builds a lake
    assert not list(tmp_path.rglob("*.ducklake"))


def test_fetch_full_only(tmp_path, monkeypatch):
    _patch(monkeypatch)
    out = tmp_path / "out"

    result = runner.invoke(app, ["fetch", "brown", str(out), "--full-only"])
    assert result.exit_code == 0, result.output

    parquets = sorted(p.name for p in out.glob("*.parquet"))
    assert parquets == ["brown-2026-02-11-full-marcxml.parquet"]
    assert not list(out.glob("*.del.txt"))
