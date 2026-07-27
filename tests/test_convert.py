import gzip
from pathlib import Path

import pandas
from lxml import etree

from podlake.convert import (
    _iter_marcxml_records,
    _marc_element_to_record,
    dump_to_parquet,
)

RECORD_XML = (
    '<record xmlns="http://www.loc.gov/MARC21/slim">'
    "<leader>00000nam a2200000 a 4500</leader>"
    '<controlfield tag="001">{id}</controlfield>'
    '<datafield tag="245" ind1="0" ind2="0">'
    '<subfield code="a">{title}</subfield></datafield>'
    "</record>"
)


def _collection_gz(path: Path, records: list[tuple[str, str]]) -> None:
    body = "".join(RECORD_XML.format(id=i, title=t) for i, t in records)
    xml = f'<collection xmlns="http://www.loc.gov/MARC21/slim">{body}</collection>'
    with gzip.open(path, "wb") as fh:
        fh.write(xml.encode("utf-8"))


def test_marc_element_to_record():
    el = etree.fromstring(RECORD_XML.format(id="a1", title="Hello"))
    record = _marc_element_to_record(el)
    assert record is not None
    assert record["001"].data == "a1"
    assert record["245"]["a"] == "Hello"


def test_iter_marcxml_records_streams_gz(tmp_path):
    path = tmp_path / "dump.xml.gz"
    _collection_gz(path, [("a1", "One"), ("a2", "Two")])
    records = list(_iter_marcxml_records(path))
    assert [r["001"].data for r in records] == ["a1", "a2"]


def test_dump_to_parquet(tmp_path):
    gz = tmp_path / "brown-2026-02-11-full-marcxml.xml.gz"
    _collection_gz(gz, [("a1", "One"), ("a2", "Two")])
    out = tmp_path / "out.parquet"

    dump_to_parquet("brown", gz, out, batch_size=10)

    df = pandas.read_parquet(out)
    assert len(df) == 2
    assert df["pod_record_id"].tolist() == ["brown:a1", "brown:a2"]
    assert df["F245"].tolist() == ["One", "Two"]
    assert "goldrush_key" in df.columns


def test_dump_to_parquet_limit(tmp_path):
    gz = tmp_path / "d.xml.gz"
    _collection_gz(gz, [("a1", "One"), ("a2", "Two"), ("a3", "Three")])
    out = tmp_path / "o.parquet"

    dump_to_parquet("brown", gz, out, limit=2)

    assert len(pandas.read_parquet(out)) == 2
