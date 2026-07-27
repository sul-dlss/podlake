import gzip
import logging
from collections.abc import Iterator
from pathlib import Path

import pymarc
from goldrush import goldrush
from lxml import etree
from lxml.etree import QName, tostring
from lxml.etree import _Element as Element
from marctable import Column, ColumnSpec, to_parquet
from marctable.marc import MARC

logger = logging.getLogger(__name__)

MARC_NS = "http://www.loc.gov/MARC21/slim"


def dump_to_parquet(
    org: str,
    marcxml_path: Path,
    parquet_path: Path,
    batch_size: int = 100_000,
    on_record=None,
    limit: int | None = None,
) -> Path:
    """
    Convert a downloaded MARCXML dump file (a ResourceSync full or delta dump,
    optionally gzipped) into a Parquet file, keyed the same way as every other
    podlake table via `_make_columns` (pod_record_id, goldrush_key, and the wide
    MARC field/subfield columns).

    Records are streamed one at a time (see `_iter_marcxml_records`) so memory
    stays bounded regardless of dump size; `batch_size` controls the Parquet
    row-group buffer, the real memory driver on the wide MARC schema.
    """
    columns = _make_columns(org)
    records = _dump_record_iterator(marcxml_path, on_record=on_record, limit=limit)
    to_parquet(records, parquet_path.open("wb"), columns=columns, batch_size=batch_size)
    return parquet_path


def _dump_record_iterator(
    marcxml_path: Path, on_record=None, limit: int | None = None
) -> Iterator[pymarc.Record]:
    for count, record in enumerate(_iter_marcxml_records(marcxml_path)):
        if limit is not None and count >= limit:
            break
        if on_record:
            on_record(count + 1)
        yield record


def _iter_marcxml_records(path: Path) -> Iterator[pymarc.Record]:
    """
    Stream `pymarc.Record`s from a MARCXML file, transparently handling gzip.
    Uses lxml `iterparse` and clears elements as it goes so a multi-GB dump is
    parsed in constant memory.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as fh:
        for _event, el in etree.iterparse(
            fh, events=("end",), tag=f"{{{MARC_NS}}}record"
        ):
            record = _marc_element_to_record(el)
            if record is not None:
                yield record
            # free the element and any preceding siblings to bound memory
            el.clear()
            while el.getprevious() is not None:
                del el.getparent()[0]


def _marc_element_to_record(marc_el: Element) -> pymarc.Record | None:
    """
    Construct a pymarc.Record from a MARCXML `record` element (namespace-agnostic
    via local names). Shared by every ingestion source.
    """
    record = pymarc.Record()

    for child in marc_el:
        local = QName(child).localname

        if local == "leader":
            record.leader = pymarc.Leader(child.text or "")
        elif local == "controlfield":
            tag = child.get("tag")
            if tag is None:
                logger.warning(
                    f"Skipping controlfield without a tag: {tostring(child)}"
                )
                continue
            field = pymarc.Field(tag)
            field.data = child.text or ""
            record.add_field(field)
        elif local == "datafield":
            tag = child.get("tag")
            if tag is None:
                logger.warning(f"Skipping datafield without a tag: {tostring(child)}")
                continue
            field = pymarc.Field(
                tag,
                pymarc.Indicators(child.get("ind1", " "), child.get("ind2", " ")),
            )
            for subfield in child:
                code = subfield.get("code")
                if code is None:
                    logger.warning(
                        f"Skipping subfield without a code: {tostring(subfield)}"
                    )
                    continue
                field.add_subfield(code, subfield.text or "")
            record.add_field(field)

    return record


def _make_columns(set_name: str) -> list[ColumnSpec]:
    marc_schema = MARC.from_avram()

    def _make_id(rec):
        return f"{set_name}:{rec['001'].data.strip()}"

    rules: list[ColumnSpec] = [
        Column("pod_record_id", _make_id),
        Column("goldrush_key", goldrush),
    ]

    for field in marc_schema.fields:
        rules.append(field.tag)
        if field.subfields:
            subfield_codes = "".join([subfield.code for subfield in field.subfields])
            rules.append(f"{field.tag}{subfield_codes}")

    return rules
