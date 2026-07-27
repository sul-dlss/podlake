import os
from datetime import UTC, datetime
from pathlib import Path

import boto3
import duckdb
import moto
import pytest

from podlake import lake
from podlake.config import Config


def _dev_config(tmp_path: Path) -> Config:
    return Config(
        env="development",
        data_path=str(tmp_path / "data") + "/",
        catalog_uri=str(tmp_path / "podlake.ducklake"),
    )


def _records_parquet(path: Path, rows: list[tuple]) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t (org VARCHAR, pod_record_id VARCHAR, field_tag VARCHAR, "
        "field_seq INTEGER, ind1 VARCHAR, ind2 VARCHAR, subfield_code VARCHAR, "
        "subfield_seq INTEGER, value VARCHAR)"
    )
    if rows:
        con.executemany("INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?)", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()


def _meta_parquet(path: Path, rows: list[tuple]) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE m (org VARCHAR, pod_record_id VARCHAR, goldrush_key VARCHAR)"
    )
    if rows:
        con.executemany("INSERT INTO m VALUES (?,?,?)", rows)
    con.execute(f"COPY m TO '{path}' (FORMAT parquet)")
    con.close()


def _record(org, rid, title, gr):
    """Return (eav_rows, meta_row) for one simple record: LDR + 001 + 245$a."""
    pid = f"{org}:{rid}"
    rows = [
        (org, pid, "LDR", 0, None, None, None, None, "00000nam a2200000 a 4500"),
        (org, pid, "001", 1, None, None, None, None, rid),
        (org, pid, "245", 2, "1", "0", "a", 0, title),
    ]
    return rows, (org, pid, gr)


def _write_org(tmp_path, name, records):
    """Write a records+meta parquet pair for a list of _record() results."""
    rec_rows = [r for rec, _ in records for r in rec]
    meta_rows = [meta for _, meta in records]
    rpq = tmp_path / f"{name}.records.parquet"
    mpq = tmp_path / f"{name}.meta.parquet"
    _records_parquet(rpq, rec_rows)
    _meta_parquet(mpq, meta_rows)
    return rpq, mpq


def test_load_pair_and_overlap(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))

    srpq, smpq = _write_org(
        tmp_path,
        "stanford",
        [
            _record("stanford", "a1", "Symphony", "shared"),
            _record("stanford", "a2", "Sonata", "stanford_only"),
        ],
    )
    hrpq, hmpq = _write_org(
        tmp_path, "harvard", [_record("harvard", "b1", "Symphony", "shared")]
    )

    assert lake.load_pair(con, "stanford", srpq, smpq) == 2
    assert lake.load_pair(con, "harvard", hrpq, hmpq) == 1

    # records partitioned by org
    cols = [r[0] for r in con.execute("DESCRIBE records").fetchall()]
    assert cols[0] == "org"

    counts = dict(
        con.execute("SELECT org, count(*) FROM record_meta GROUP BY org").fetchall()
    )
    assert counts == {"stanford": 2, "harvard": 1}

    # consortial overlap via record_meta
    overlap = con.execute(
        "SELECT goldrush_key, count(DISTINCT org) AS orgs FROM record_meta "
        "GROUP BY goldrush_key HAVING orgs > 1"
    ).fetchall()
    assert overlap == [("shared", 2)]
    con.close()


def test_apply_resource_upsert_and_delete(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    srpq, smpq = _write_org(
        tmp_path,
        "stanford",
        [
            _record("stanford", "a1", "Symphony", "k1"),
            _record("stanford", "a2", "Sonata", "k2"),
        ],
    )
    lake.load_pair(con, "stanford", srpq, smpq)

    # delta: a1 retitled, a3 new
    drpq, dmpq = _write_org(
        tmp_path,
        "stanford-delta",
        [
            _record("stanford", "a1", "Symphony (rev)", "k1"),
            _record("stanford", "a3", "Concerto", "k3"),
        ],
    )
    ts1 = datetime(2026, 2, 12, tzinfo=UTC)
    changed, _ = lake.apply_resource(con, "stanford", "delta", (drpq, dmpq), ts1)
    assert changed == 2

    ts2 = datetime(2026, 2, 13, tzinfo=UTC)
    _, deleted = lake.apply_resource(con, "stanford", "deletes", ["stanford:a2"], ts2)
    assert deleted == 1

    ids = [
        r[0]
        for r in con.execute(
            "SELECT pod_record_id FROM record_meta ORDER BY pod_record_id"
        ).fetchall()
    ]
    assert ids == ["stanford:a1", "stanford:a3"]

    title = con.execute(
        "SELECT value FROM records WHERE pod_record_id='stanford:a1' "
        "AND field_tag='245' AND subfield_code='a'"
    ).fetchone()
    assert title == ("Symphony (rev)",)

    assert lake.get_cursor(con, "stanford") == ts2
    con.close()


def test_apply_resource_empty_delta(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    srpq, smpq = _write_org(
        tmp_path, "stanford", [_record("stanford", "a1", "Symphony", "k1")]
    )
    lake.load_pair(con, "stanford", srpq, smpq)

    # an empty delta (0 records) is a no-op that still advances the cursor
    erpq, empq = _write_org(tmp_path, "empty", [])
    ts = datetime(2026, 3, 1, tzinfo=UTC)
    changed, deleted = lake.apply_resource(con, "stanford", "delta", (erpq, empq), ts)
    assert (changed, deleted) == (0, 0)
    assert con.execute("SELECT count(*) FROM record_meta").fetchone() == (1,)
    assert lake.get_cursor(con, "stanford") == ts
    con.close()


def test_eav_query_shapes(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    rows = [
        ("stanford", "stanford:a1", "LDR", 0, None, None, None, None, "00000nam"),
        ("stanford", "stanford:a1", "001", 1, None, None, None, None, "a1"),
        ("stanford", "stanford:a1", "245", 2, "1", "0", "a", 0, "Symphony"),
        ("stanford", "stanford:a1", "245", 2, "1", "0", "b", 1, "in D"),
        ("stanford", "stanford:a1", "100", 3, "1", " ", "a", 0, "Beethoven"),
        ("stanford", "stanford:a1", "650", 4, " ", "0", "a", 0, "Music"),
        ("stanford", "stanford:a1", "650", 5, " ", "0", "a", 0, "History"),
        ("stanford", "stanford:a2", "LDR", 0, None, None, None, None, "00000nam"),
        ("stanford", "stanford:a2", "001", 1, None, None, None, None, "a2"),
        ("stanford", "stanford:a2", "245", 2, "1", "0", "a", 0, "Sonata"),
        ("stanford", "stanford:a2", "100", 3, "1", " ", "a", 0, "Mozart"),
    ]
    rpq = tmp_path / "s.records.parquet"
    mpq = tmp_path / "s.meta.parquet"
    _records_parquet(rpq, rows)
    _meta_parquet(
        mpq, [("stanford", "stanford:a1", "k1"), ("stanford", "stanford:a2", "k2")]
    )
    lake.load_pair(con, "stanford", rpq, mpq)

    # whole 245 field via string_agg (subfields joined in subfield order)
    whole = con.execute(
        "SELECT string_agg(value, ' ' ORDER BY subfield_seq) FROM records "
        "WHERE pod_record_id='stanford:a1' AND field_tag='245'"
    ).fetchone()
    assert whole == ("Symphony in D",)

    # self-join: title <-> author
    sj = con.execute(
        "SELECT t.value, a.value FROM records t JOIN records a USING (pod_record_id) "
        "WHERE t.field_tag='245' AND t.subfield_code='a' "
        "AND a.field_tag='100' AND a.subfield_code='a' "
        "AND t.pod_record_id='stanford:a1'"
    ).fetchone()
    assert sj == ("Symphony", "Beethoven")

    # FILTER-agg pivot: title per record
    pivot = dict(
        con.execute(
            "SELECT pod_record_id, "
            "max(value) FILTER (WHERE field_tag='245' AND subfield_code='a') "
            "FROM records GROUP BY pod_record_id"
        ).fetchall()
    )
    assert pivot == {"stanford:a1": "Symphony", "stanford:a2": "Sonata"}

    # repeated 650 preserved as two rows with distinct field_seq
    counts = con.execute(
        "SELECT count(*), count(DISTINCT field_seq) FROM records "
        "WHERE pod_record_id='stanford:a1' AND field_tag='650'"
    ).fetchone()
    assert counts == (2, 2)
    con.close()


def test_get_cursor_absent(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    assert lake.get_cursor(con, "stanford") is None
    con.close()


def test_read_only_cannot_write(tmp_path):
    config = _dev_config(tmp_path)
    rpq, mpq = _write_org(tmp_path, "stanford", [_record("stanford", "a1", "T", "k")])

    writer = lake.connect(read_only=False, config=config)
    lake.load_pair(writer, "stanford", rpq, mpq)
    writer.close()

    reader = lake.connect(read_only=True, config=config)
    assert reader.execute("SELECT count(*) FROM record_meta").fetchone() == (1,)
    with pytest.raises(duckdb.Error):
        reader.execute("INSERT INTO record_meta VALUES ('x','x:1','k')")
    reader.close()


def test_publish_uploads_catalog(tmp_path):
    config = _dev_config(tmp_path)
    rpq, mpq = _write_org(tmp_path, "stanford", [_record("stanford", "a1", "T", "k")])
    con = lake.connect(read_only=False, config=config)
    lake.load_pair(con, "stanford", rpq, mpq)
    con.close()

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    with moto.mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="pod-public")
        catalog_key, _, count = lake.publish(config, "s3://pod-public/lake")
        assert count >= 1
        keys = [
            o["Key"]
            for o in s3.list_objects_v2(Bucket="pod-public").get("Contents", [])
        ]
        assert f"lake/{catalog_key}" in keys


def test_publish_rejects_postgres_catalog(tmp_path):
    pg_config = Config(
        env="production",
        data_path="s3://x/lake/",
        catalog_uri="postgres:dbname=podlake host=db",
    )
    with pytest.raises(ValueError):
        lake.publish(pg_config, "s3://pod-public/lake")
