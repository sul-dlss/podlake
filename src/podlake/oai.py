import os
from collections.abc import Iterator
from typing import Optional

from sickle import Sickle
from sickle.models import Record, Set

XML_NS = {
    "marc": "http://www.loc.gov/MARC21/slim",
    "oai": "http://www.openarchives.org/OAI/2.0/",
}


def list_sets() -> list[Set]:
    oai = Sickle("https://pod.stanford.edu/oai", headers=_headers())
    return list(oai.ListSets())


def get_set(name: str) -> Set | None:
    for s in list_sets():
        if s.contributor.lower() == name.lower():  # ty: ignore[unresolved-attribute]
            return s
    return None


def list_records(set_id: str, from_: Optional[str] = None) -> Iterator[Record]:
    oai = Sickle("https://pod.stanford.edu/oai", headers=_headers())

    # we are going to get marc21 records for the set
    params = {
        "metadataPrefix": "marc21",
        "set": set_id,
    }

    # optionally add the from date
    if from_:
        params["from"] = from_

    yield from oai.ListRecords(**params)


def _headers() -> dict[str, str]:
    token = os.environ.get("PODBUCKET_POD_TOKEN")
    if token is None:
        raise Exception("PODBUCKET_POD_TOKEN env var isn't set!")
    return {"Authorization": f"Bearer {token}"}
