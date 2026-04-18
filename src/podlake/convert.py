import logging
from collections.abc import Iterator
from pathlib import Path

import pymarc
from lxml.etree import QName, _Element as Element, tostring
from marctable import to_parquet, Column, ColumnSpec
from marctable.marc import MARC

from podlake import oai

logger = logging.getLogger(__name__)


def oai_to_parquet(set_name: str, parquet_path: Path, limit=None, on_record=None):
    """
    Pass in the name of the collection to harvest, a path to a Parquet file
    and an optional record limit (useful in testing). An on_record parameter can
    be used if you'd like to run a function for every record that is harvested,
    which is useful for a progress bar in the CLI.
    """
    set_ = oai.get_set(set_name)
    if set_ is None:
        raise Exception("Unknown pod set name {name}")
    set_id = set_.setSpec  # ty: ignore[unresolved-attribute]

    columns = _make_columns(set_name)
    records = _record_iterator(set_id, limit=limit, on_record=on_record)
    to_parquet(records, parquet_path.open("wb"), columns=columns, batch_size=100_000)

    return parquet_path


def _record_iterator(
    set_id: str, limit: int | None = None, on_record=None
) -> Iterator[pymarc.Record]:
    seen = set()
    for count, oai_record in enumerate(oai.list_records(set_id)):
        if limit is not None and count >= limit:
            break

        rec_id = oai_record.header.identifier
        logging.debug(f"found oai record {count} with ID {rec_id}")

        if rec_id in seen:
            logging.warning(f"already saw oai record! {rec_id}")
        else:
            seen.add(rec_id)

        if on_record:
            on_record(count + 1)

        marc_rec = _oai_to_marc_record(oai_record.xml)

        if marc_rec is not None:
            yield marc_rec


def _oai_to_marc_record(el: Element) -> pymarc.Record | None:
    """
    Construct a pymarc.Record object based on the MARCXML in an OAI record.
    """
    record = pymarc.Record()
    marc_el = el.find(".//marc:record", namespaces=oai.XML_NS)
    if marc_el is None:
        logging.warning(f"No MARC XML record found in XML: {tostring(el)}")
        return None

    for child in marc_el:
        local = QName(child).localname

        if local == "leader":
            record.leader = pymarc.Leader(child.text or "")
        elif local == "controlfield":
            field = pymarc.Field(child.get("tag"))
            field.data = child.text or ""
            record.add_field(field)
        elif local == "datafield":
            field = pymarc.Field(
                child.get("tag"),
                pymarc.Indicators(child.get("ind1", " "), child.get("ind2", " ")),
            )
            for subfield in child:
                field.add_subfield(subfield.get("code"), subfield.text or "")
            record.add_field(field)

    return record


def _make_columns(set_name: str) -> list[ColumnSpec]:
    marc_schema = MARC.from_avram()

    def _make_id(rec):
        return f"{set_name}:{rec['001'].data.strip()}"

    rules: list[ColumnSpec] = [Column("pod_record_id", _make_id)]

    for field in marc_schema.fields:
        rules.append(field.tag)
        if field.subfields:
            subfield_codes = "".join([subfield.code for subfield in field.subfields])
            rules.append(f"{field.tag}{subfield_codes}")

    return rules
