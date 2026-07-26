import logging
from collections.abc import Iterator
from pathlib import Path

import pymarc
from goldrush import goldrush
from lxml.etree import QName, tostring
from lxml.etree import _Element as Element
from marctable import Column, ColumnSpec, to_parquet
from marctable.marc import MARC
from sickle.oaiexceptions import NoRecordsMatch

from podlake import oai

logger = logging.getLogger(__name__)


def oai_to_parquet(
    set_name: str,
    parquet_path: Path,
    limit=None,
    on_record=None,
    from_: str | None = None,
    deleted: list[str] | None = None,
):
    """
    Pass in the name of the collection to harvest, a path to a Parquet file
    and an optional record limit (useful in testing). An on_record parameter can
    be used if you'd like to run a function for every record that is harvested,
    which is useful for a progress bar in the CLI.

    Pass from_ (a YYYY-MM-DD date string) to only harvest records changed on or
    after that date, which is how incremental updates are performed. When
    harvesting a delta, pass a list as `deleted` to collect the pod_record_ids
    of records POD reports as deleted, so they can be removed downstream.
    """
    set_ = oai.get_set(set_name)
    if set_ is None:
        raise ValueError(f"Unknown pod set name {set_name}")
    set_id = set_.setSpec  # ty: ignore[unresolved-attribute]

    columns = _make_columns(set_name)
    records = _record_iterator(
        set_id,
        set_name,
        limit=limit,
        on_record=on_record,
        from_=from_,
        deleted=deleted,
    )
    to_parquet(records, parquet_path.open("wb"), columns=columns, batch_size=100_000)

    return parquet_path


def _record_iterator(
    set_id: str,
    org: str,
    limit: int | None = None,
    on_record=None,
    from_: str | None = None,
    deleted: list[str] | None = None,
) -> Iterator[pymarc.Record]:
    # TODO: use on disk dictionary?
    seen = set()
    try:
        oai_records = enumerate(oai.list_records(set_id, from_=from_))
        for count, oai_record in oai_records:
            if limit is not None and count >= limit:
                break

            rec_id = oai_record.header.identifier
            logger.debug(f"found oai record {count} with ID {rec_id}")

            if rec_id is None:
                logger.warning(f"skipping oai record {count} without an identifier")
                continue

            if rec_id in seen:
                logger.info(f"skipping previous version of oai record {rec_id}")
                continue
            else:
                seen.add(rec_id)

            if on_record:
                on_record(count + 1)

            # POD reports deletions as records with a deleted header and no MARC
            # body. Collect their pod_record_id so they can be removed from the
            # lake, and don't yield them for conversion.
            if oai_record.deleted:
                if deleted is not None:
                    deleted.append(_deleted_pod_record_id(org, rec_id))
                continue

            marc_rec = _oai_to_marc_record(oai_record.xml)

            if marc_rec is not None:
                yield marc_rec
    except NoRecordsMatch:
        # An empty delta (nothing changed since from_) is not an error.
        logger.info(f"no records match for set {set_id} from {from_}")
        return


def _deleted_pod_record_id(org: str, identifier: str) -> str:
    """
    Derive a pod_record_id from an OAI identifier for a deleted record, which
    carries no MARC body. The identifier's final colon-delimited segment is the
    MARC 001 control number, e.g. "oai:pod.stanford.edu:stanford:a1" -> the
    pod_record_id "stanford:a1", matching how ids are minted for live records.
    """
    local_id = identifier.split(":")[-1]
    return f"{org}:{local_id}"


def _oai_to_marc_record(el: Element) -> pymarc.Record | None:
    """
    Construct a pymarc.Record object based on the MARCXML in an OAI record.
    """
    record = pymarc.Record()
    marc_el = el.find(".//marc:record", namespaces=oai.XML_NS)
    if marc_el is None:
        logger.warning(f"No MARC XML record found in XML: {tostring(el)}")
        return None

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
