import os
from datetime import date
from pathlib import Path

import boto3
import duckdb
import moto
import pandas
import pytest

from podlake import lake
from podlake.config import Config


def _dev_config(tmp_path: Path) -> Config:
    return Config(
        env="development",
        data_path=str(tmp_path / "data") + "/",
        catalog_uri=str(tmp_path / "podlake.ducklake"),
    )


def _make_parquet(path: Path, rows: list[dict]) -> None:
    pandas.DataFrame(rows).to_parquet(path)


@pytest.fixture
def parquet_files(tmp_path):
    stanford = tmp_path / "stanford.parquet"
    harvard = tmp_path / "harvard.parquet"
    _make_parquet(
        stanford,
        [
            {
                "pod_record_id": "stanford:a1",
                "goldrush_key": "shared_key",
                "F245": "Symphony",
            },
            {
                "pod_record_id": "stanford:a2",
                "goldrush_key": "stanford_only",
                "F245": "Sonata",
            },
        ],
    )
    _make_parquet(
        harvard,
        [
            {
                "pod_record_id": "harvard:b1",
                "goldrush_key": "shared_key",
                "F245": "Symphony",
            },
        ],
    )
    return stanford, harvard


def test_load_and_query(tmp_path, parquet_files):
    stanford, harvard = parquet_files
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))

    assert lake.load_parquet(con, stanford, "stanford") == 2
    assert lake.load_parquet(con, harvard, "harvard") == 1

    # the records table exists and is partitioned by an org column
    columns = [row[0] for row in con.execute("DESCRIBE records").fetchall()]
    assert columns[0] == "org"
    assert "pod_record_id" in columns
    assert "goldrush_key" in columns

    # per-org row counts
    counts = dict(
        con.execute("SELECT org, count(*) FROM records GROUP BY org").fetchall()
    )
    assert counts == {"stanford": 2, "harvard": 1}

    # cross-org overlap via goldrush_key works in a single query
    overlap = con.execute(
        "SELECT goldrush_key, count(DISTINCT org) AS orgs FROM records "
        "GROUP BY goldrush_key HAVING orgs > 1"
    ).fetchall()
    assert overlap == [("shared_key", 2)]

    con.close()


def test_reload_is_idempotent(tmp_path, parquet_files):
    stanford, _ = parquet_files
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))

    lake.load_parquet(con, stanford, "stanford")
    lake.load_parquet(con, stanford, "stanford")

    total = con.execute("SELECT count(*) FROM records").fetchone()
    assert total is not None and total[0] == 2

    con.close()


def test_apply_update_upserts_and_deletes(tmp_path, parquet_files):
    stanford, _ = parquet_files
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    lake.load_parquet(con, stanford, "stanford")

    # a delta: a1 changes title, a3 is new; a2 will be deleted
    delta = tmp_path / "stanford-delta.parquet"
    _make_parquet(
        delta,
        [
            {
                "pod_record_id": "stanford:a1",
                "goldrush_key": "shared_key",
                "F245": "Symphony (revised)",
            },
            {
                "pod_record_id": "stanford:a3",
                "goldrush_key": "new_key",
                "F245": "Concerto",
            },
        ],
    )

    changed, deleted = lake.apply_update(
        con, "stanford", delta, ["stanford:a2"], date(2026, 7, 26)
    )
    assert (changed, deleted) == (2, 1)

    rows = dict(
        con.execute(
            "SELECT pod_record_id, F245 FROM records WHERE org = 'stanford' "
            "ORDER BY pod_record_id"
        ).fetchall()
    )
    # a1 updated in place (no duplicate), a2 removed, a3 added
    assert rows == {
        "stanford:a1": "Symphony (revised)",
        "stanford:a3": "Concerto",
    }

    assert lake.get_last_harvest(con, "stanford") == date(2026, 7, 26)
    con.close()


def test_publish_uploads_catalog_and_data(tmp_path, parquet_files):
    stanford, _ = parquet_files
    config = _dev_config(tmp_path)

    con = lake.connect(read_only=False, config=config)
    lake.load_parquet(con, stanford, "stanford")
    con.close()  # flush the catalog file so it can be uploaded

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    with moto.mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="pod-public")

        catalog_key, data_prefix, count = lake.publish(config, "s3://pod-public/lake")

        assert catalog_key == Path(config.catalog_uri).name
        assert data_prefix == "data"
        assert count >= 1
        keys = [
            o["Key"]
            for o in s3.list_objects_v2(Bucket="pod-public").get("Contents", [])
        ]
        # the catalog is always published under the destination prefix
        assert f"lake/{catalog_key}" in keys


def test_publish_rejects_postgres_catalog(tmp_path):
    pg_config = Config(
        env="production",
        data_path="s3://x/lake/",
        catalog_uri="postgres:dbname=podlake host=db",
    )
    with pytest.raises(ValueError):
        lake.publish(pg_config, "s3://pod-public/lake")


def test_get_last_harvest_absent(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    assert lake.get_last_harvest(con, "stanford") is None
    con.close()


def test_read_only_cannot_write(tmp_path, parquet_files):
    stanford, _ = parquet_files
    config = _dev_config(tmp_path)

    writer = lake.connect(read_only=False, config=config)
    lake.load_parquet(writer, stanford, "stanford")
    writer.close()

    reader = lake.connect(read_only=True, config=config)
    assert reader.execute("SELECT count(*) FROM records").fetchone() == (2,)
    with pytest.raises(duckdb.Error):
        reader.execute("INSERT INTO records (org, pod_record_id) VALUES ('x', 'x:1')")
    reader.close()
