import pandas
from sickle.oaiexceptions import NoRecordsMatch

from podlake import convert
from podlake.convert import _deleted_pod_record_id, oai_to_parquet


class _FakeHeader:
    def __init__(self, identifier):
        self.identifier = identifier


class _FakeRecord:
    def __init__(self, identifier, deleted, xml=None):
        self.header = _FakeHeader(identifier)
        self.deleted = deleted
        self.xml = xml


def test_deleted_pod_record_id():
    identifier = "oai:pod.stanford.edu:stanford:a1"
    assert _deleted_pod_record_id("stanford", identifier) == "stanford:a1"


def test_record_iterator_collects_deletions_and_skips_them(monkeypatch):
    records = [
        _FakeRecord("oai:pod.stanford.edu:stanford:a1", deleted=False, xml="rec-a1"),
        _FakeRecord("oai:pod.stanford.edu:stanford:a2", deleted=True),
        _FakeRecord("oai:pod.stanford.edu:stanford:a3", deleted=False, xml="rec-a3"),
    ]
    monkeypatch.setattr(
        convert.oai, "list_records", lambda set_id, from_=None: iter(records)
    )
    # bypass MARCXML parsing: yield the sentinel xml straight through
    monkeypatch.setattr(convert, "_oai_to_marc_record", lambda xml: xml)

    deleted: list[str] = []
    yielded = list(convert._record_iterator("503", "stanford", deleted=deleted))

    assert yielded == ["rec-a1", "rec-a3"], "deleted record is not yielded"
    assert deleted == ["stanford:a2"], "deleted id mapped from the OAI identifier"


def test_record_iterator_empty_delta_is_not_an_error(monkeypatch):
    def fake(set_id, from_=None):
        if False:  # make this a generator that raises when iterated
            yield
        raise NoRecordsMatch("empty")

    monkeypatch.setattr(convert.oai, "list_records", fake)

    deleted: list[str] = []
    assert list(convert._record_iterator("503", "stanford", deleted=deleted)) == []
    assert deleted == []


def test_convert(tmp_path):
    parquet_path = tmp_path / "test.parquet"
    oai_to_parquet("stanford", parquet_path=parquet_path, limit=2000)
    assert parquet_path.is_file()

    df = pandas.read_parquet(parquet_path)
    assert len(df) == 2000

    assert df["pod_record_id"].iloc[0] == "stanford:a1"
    assert (
        df["goldrush_key"].iloc[0]
        == "symphonyop38________________________________________________________________________________________1967_______roberc________________________________________schum________________v03182026p"
    )
    assert df["F245"].iloc[0] == "Symphony, op. 38"

    assert df["pod_record_id"].iloc[1] == "stanford:a10"
    assert (
        df["goldrush_key"].iloc[1]
        == "paniscifistulatrepreludipertreflauti________________________________________________________________1973_______c________________________________________novak________________v03182026p"
    )

    assert df["F245"].iloc[1] == "Panisci fistula; tre preludi per tre flauti."
