import gzip
import logging
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pymarc
from goldrush import goldrush
from lxml import etree
from lxml.etree import QName, tostring
from lxml.etree import _Element as Element

logger = logging.getLogger(__name__)

MARC_NS = "http://www.loc.gov/MARC21/slim"

# The tall/EAV `records` schema: one row per subfield (plus a row for the leader
# and one per control field). field_seq + subfield_seq preserve order and
# repeats, so a record is exactly reconstructable.
RECORDS_SCHEMA = pa.schema(
    [
        ("org", pa.string()),
        ("pod_record_id", pa.string()),
        ("field_tag", pa.string()),
        ("field_seq", pa.int32()),
        ("ind1", pa.string()),
        ("ind2", pa.string()),
        ("subfield_code", pa.string()),
        ("subfield_seq", pa.int32()),
        ("value", pa.string()),
    ]
)

# Per-record, podlake-derived attributes (one row per record).
META_SCHEMA = pa.schema(
    [
        ("org", pa.string()),
        ("pod_record_id", pa.string()),
        ("goldrush_key", pa.string()),
    ]
)


def dump_to_parquet(
    org: str,
    marcxml_path: Path,
    records_out: Path,
    meta_out: Path,
    batch_size: int = 100_000,
    on_record=None,
    limit: int | None = None,
) -> tuple[Path, Path]:
    """
    Convert a downloaded MARCXML dump into two Parquet files: the tall `records`
    EAV table (`records_out`) and the per-record `record_meta` table
    (`meta_out`). Records are streamed and flushed every `batch_size` records so
    memory stays bounded regardless of dump size.
    """
    rec_writer = pq.ParquetWriter(
        str(records_out), RECORDS_SCHEMA, compression="snappy"
    )
    meta_writer = pq.ParquetWriter(str(meta_out), META_SCHEMA, compression="snappy")
    rbuf: dict[str, list] = {name: [] for name in RECORDS_SCHEMA.names}
    mbuf: dict[str, list] = {name: [] for name in META_SCHEMA.names}

    count = 0
    try:
        for record in _iter_marcxml_records(marcxml_path):
            if limit is not None and count >= limit:
                break
            result = record_to_rows(org, record)
            if result is None:
                continue
            eav_rows, meta_row = result
            for row in eav_rows:
                for name in RECORDS_SCHEMA.names:
                    rbuf[name].append(row[name])
            for name in META_SCHEMA.names:
                mbuf[name].append(meta_row[name])

            count += 1
            if on_record:
                on_record(count)
            if count % batch_size == 0:
                _flush(rec_writer, RECORDS_SCHEMA, rbuf)
                _flush(meta_writer, META_SCHEMA, mbuf)

        _flush(rec_writer, RECORDS_SCHEMA, rbuf)
        _flush(meta_writer, META_SCHEMA, mbuf)
    finally:
        rec_writer.close()
        meta_writer.close()

    return records_out, meta_out


def _flush(writer: pq.ParquetWriter, schema: pa.Schema, buf: dict[str, list]) -> None:
    if not buf[schema.names[0]]:
        return
    writer.write_table(
        pa.table({name: buf[name] for name in schema.names}, schema=schema)
    )
    for name in schema.names:
        buf[name].clear()


def record_to_rows(org: str, record: pymarc.Record) -> tuple[list[dict], dict] | None:
    """
    Turn a pymarc.Record into (EAV rows, meta row). Returns None (with a warning)
    if the record has no 001, since pod_record_id depends on it.
    """
    control_001 = record.get_fields("001")
    if not control_001 or control_001[0].data is None:
        logger.warning("skipping record without a usable 001 control field")
        return None
    pod_record_id = f"{org}:{control_001[0].data.strip()}"

    def eav(field_tag, field_seq, value, ind1=None, ind2=None, code=None, sf_seq=None):
        return {
            "org": org,
            "pod_record_id": pod_record_id,
            "field_tag": field_tag,
            "field_seq": field_seq,
            "ind1": ind1,
            "ind2": ind2,
            "subfield_code": code,
            "subfield_seq": sf_seq,
            "value": value,
        }

    # leader first (field_seq 0), then the fields in order
    rows = [eav("LDR", 0, str(record.leader))]
    for i, field in enumerate(record.fields, start=1):
        if field.is_control_field():
            rows.append(eav(field.tag, i, field.data))
        elif field.subfields:
            for j, subfield in enumerate(field.subfields):
                rows.append(
                    eav(
                        field.tag,
                        i,
                        subfield.value,
                        ind1=field.indicator1,
                        ind2=field.indicator2,
                        code=subfield.code,
                        sf_seq=j,
                    )
                )
        else:
            # a data field with no subfields: keep its presence + indicators
            rows.append(
                eav(field.tag, i, None, ind1=field.indicator1, ind2=field.indicator2)
            )

    meta_row = {
        "org": org,
        "pod_record_id": pod_record_id,
        "goldrush_key": goldrush(record),
    }
    return rows, meta_row


def _iter_marcxml_records(path: Path) -> Iterator[pymarc.Record]:
    """
    Stream `pymarc.Record`s from a MARCXML file (gzip-aware), clearing elements
    as it goes so a multi-GB dump is parsed in constant memory. This is the
    isolated parse seam — swappable for a faster parser later.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as fh:
        for _event, el in etree.iterparse(
            fh, events=("end",), tag=f"{{{MARC_NS}}}record"
        ):
            record = _marc_element_to_record(el)
            if record is not None:
                yield record
            el.clear()
            while el.getprevious() is not None:
                del el.getparent()[0]


def _marc_element_to_record(marc_el: Element) -> pymarc.Record | None:
    """
    Construct a pymarc.Record from a MARCXML `record` element (namespace-agnostic
    via local names).
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
