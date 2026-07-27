import gzip
from pathlib import Path

import duckdb
from lxml import etree

from podlake.convert import (
    _marc_element_to_record,
    dump_to_parquet,
    record_to_rows,
)

LEADER = "00000nam a2200000 a 4500"

RECORD_XML = (
    '<record xmlns="http://www.loc.gov/MARC21/slim">'
    f"<leader>{LEADER}</leader>"
    '<controlfield tag="001">{id}</controlfield>'
    '<controlfield tag="008">080101s2008    xxu</controlfield>'
    '<datafield tag="100" ind1="1" ind2=" "><subfield code="a">Author, A.</subfield></datafield>'
    '<datafield tag="245" ind1="1" ind2="0">'
    '<subfield code="a">{title}</subfield>'
    '<subfield code="b">a subtitle</subfield></datafield>'
    '<datafield tag="260" ind1=" " ind2=" "><subfield code="c">2008.</subfield></datafield>'
    "</record>"
)


def _collection_gz(path: Path, records: list[tuple[str, str]]) -> None:
    body = "".join(RECORD_XML.format(id=i, title=t) for i, t in records)
    xml = f'<collection xmlns="http://www.loc.gov/MARC21/slim">{body}</collection>'
    with gzip.open(path, "wb") as fh:
        fh.write(xml.encode("utf-8"))


def test_dump_to_parquet_emits_eav_and_meta(tmp_path):
    gz = tmp_path / "brown-2026-02-11-full-marcxml.xml.gz"
    _collection_gz(gz, [("a1", "Title One"), ("a2", "Title Two")])
    records_out = tmp_path / "records.parquet"
    meta_out = tmp_path / "meta.parquet"

    dump_to_parquet("brown", gz, records_out, meta_out, batch_size=10)

    con = duckdb.connect()

    # leader captured as an LDR row at field_seq 0
    leader = con.execute(
        f"SELECT value FROM read_parquet('{records_out}') "
        "WHERE pod_record_id='brown:a1' AND field_tag='LDR'"
    ).fetchone()
    assert leader == (LEADER,)

    # control field 001 -> value, no subfield/indicators
    ctrl = con.execute(
        f"SELECT value, subfield_code, ind1 FROM read_parquet('{records_out}') "
        "WHERE pod_record_id='brown:a1' AND field_tag='001'"
    ).fetchone()
    assert ctrl == ("a1", None, None)

    # data field 245: two subfields, ordered, with indicators
    subs = con.execute(
        f"SELECT subfield_code, subfield_seq, ind1, ind2, value "
        f"FROM read_parquet('{records_out}') "
        "WHERE pod_record_id='brown:a1' AND field_tag='245' "
        "ORDER BY subfield_seq"
    ).fetchall()
    assert subs == [
        ("a", 0, "1", "0", "Title One"),
        ("b", 1, "1", "0", "a subtitle"),
    ]

    # meta: one row per record with a goldrush_key
    meta = con.execute(
        f"SELECT pod_record_id, goldrush_key FROM read_parquet('{meta_out}') "
        "ORDER BY pod_record_id"
    ).fetchall()
    assert [m[0] for m in meta] == ["brown:a1", "brown:a2"]
    assert all(m[1] for m in meta), "goldrush_key populated"


def test_repeated_field_preserves_order(tmp_path):
    # a record with two 650 fields — repeats must land in distinct field_seq,
    # in source order, so the record reconstructs exactly
    rec = (
        '<record xmlns="http://www.loc.gov/MARC21/slim">'
        f"<leader>{LEADER}</leader>"
        '<controlfield tag="001">r1</controlfield>'
        '<datafield tag="650" ind1=" " ind2="0">'
        '<subfield code="a">History</subfield></datafield>'
        '<datafield tag="650" ind1=" " ind2="0">'
        '<subfield code="a">Music</subfield></datafield>'
        "</record>"
    )
    gz = tmp_path / "d.xml.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(
            f'<collection xmlns="http://www.loc.gov/MARC21/slim">{rec}</collection>'.encode()
        )
    records_out, meta_out = tmp_path / "r.parquet", tmp_path / "m.parquet"
    dump_to_parquet("brown", gz, records_out, meta_out)

    rows = (
        duckdb.connect()
        .execute(
            f"SELECT field_seq, subfield_code, value FROM read_parquet('{records_out}') "
            "WHERE field_tag='650' ORDER BY field_seq, subfield_seq"
        )
        .fetchall()
    )
    # LDR=0, 001=1, so the two 650s are field_seq 2 and 3, in order
    assert rows == [(2, "a", "History"), (3, "a", "Music")]


def test_dump_to_parquet_empty_dump(tmp_path):
    gz = tmp_path / "empty.xml.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b'<collection xmlns="http://www.loc.gov/MARC21/slim"></collection>')
    records_out, meta_out = tmp_path / "r.parquet", tmp_path / "m.parquet"

    dump_to_parquet("brown", gz, records_out, meta_out)

    con = duckdb.connect()
    assert con.execute(
        f"SELECT count(*) FROM read_parquet('{records_out}')"
    ).fetchone() == (0,)
    assert con.execute(
        f"SELECT count(*) FROM read_parquet('{meta_out}')"
    ).fetchone() == (0,)


def test_record_to_rows_skips_record_without_001():
    xml = (
        '<record xmlns="http://www.loc.gov/MARC21/slim">'
        f"<leader>{LEADER}</leader>"
        '<datafield tag="245" ind1="0" ind2="0">'
        '<subfield code="a">No control number</subfield></datafield>'
        "</record>"
    )
    record = _marc_element_to_record(etree.fromstring(xml))
    assert record is not None
    assert record_to_rows("brown", record) is None
