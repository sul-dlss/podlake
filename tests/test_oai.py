from podlake import oai
from podlake.oai import XML_NS


def test_list_sets() -> None:
    """
    Check the output of ListSets.
    """
    sets = oai.list_sets()
    assert len(sets) == 13


def test_get_set() -> None:
    """
    Make sure we can look up a set.
    """
    brown = oai.get_set("brown")
    assert brown is not None
    assert brown.setSpec == "384"  # ty: ignore[unresolved-attribute]


def test_list_records() -> None:
    """
    Use ListRecords to fetch Stanford POD records that were updated since yesterday.
    """
    # stanford is set id 503, get a maximum of 2000 records
    for count, rec in enumerate(oai.list_records("503")):
        marc = rec.xml.find("oai:metadata/marc:record", namespaces=XML_NS)
        assert marc is not None, "marc record found in oai record"

        if count == 2000:
            break

    assert count == 2000, "found 2000 records"
