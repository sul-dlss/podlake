import datetime
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import httpx
from lxml import etree
from tqdm import tqdm

logger = logging.getLogger(__name__)

POD_BASE_URL = "https://pod.stanford.edu"

# The normalized MARCXML resourcelist index: a sitemap index of per-org streams.
STREAMS_URL = f"{POD_BASE_URL}/organizations/normalized_resourcelist/marcxml"

NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "rs": "http://www.openarchives.org/rs/terms/",
}


@dataclass
class Resource:
    """
    A single ResourceSync resource: one downloadable dump file. `kind` is
    "full" (a base full dump), "delta" (a daily MARCXML delta of upserts), or
    "deletes" (a text file of deleted record ids).
    """

    url: str
    mediatype: str
    length: int
    fixity: str
    lastmod: datetime.datetime
    kind: str


def get_streams(name: str | None = None) -> dict[str, str]:
    """
    Return a mapping of organization name to its ResourceSync resourcelist URL.
    If `name` is given, only that organization is returned (case-insensitive).
    """
    doc = get_xml(STREAMS_URL)

    streams = {}
    for sitemap in doc.findall("sm:sitemap", NS):
        loc = sitemap.find("sm:loc", NS)
        if loc is None or loc.text is None:
            raise ValueError("Missing <loc> element in sitemap index")

        match = re.search(r"organizations/(.+?)/", loc.text)
        if not match:
            raise ValueError(f"Missing organization name in URL {loc.text}")
        org = match.group(1)

        if name is None or name.lower() == org.lower():
            streams[org] = loc.text

    return streams


def get_resources(resourcelist_url: str) -> list[Resource]:
    """
    Fetch a stream's resourcelist and return its resources sorted oldest-first
    by lastmod, so they can be applied in order (full dump then deltas/deletes).
    """
    doc = get_xml(resourcelist_url)

    resources = []
    for url in doc.findall("sm:url", NS):
        loc = url.find("sm:loc", NS)
        lastmod = url.find("sm:lastmod", NS)
        md = url.find("rs:md", NS)
        if (
            loc is None
            or loc.text is None
            or lastmod is None
            or lastmod.text is None
            or md is None
        ):
            raise ValueError(f"Invalid ResourceSync resource in {resourcelist_url}")

        mediatype = md.attrib.get("type", "")
        resources.append(
            Resource(
                url=loc.text,
                mediatype=mediatype,
                length=int(md.attrib.get("length", 0)),
                fixity=md.attrib.get("hash", ""),
                lastmod=datetime.datetime.fromisoformat(lastmod.text),
                kind=_classify(loc.text, mediatype),
            )
        )

    resources.sort(key=lambda r: r.lastmod)
    return resources


def _classify(url: str, mediatype: str) -> str:
    if mediatype == "text/plain" or "-deletes" in url or url.endswith(".del.txt"):
        return "deletes"
    if "-full-" in url:
        return "full"
    return "delta"


def download(
    url: str,
    path: Path,
    fixity: str | None = None,
    desc: str | None = None,
    quiet: bool = False,
) -> Path:
    """
    Stream a POD URL to a local path (following redirects to signed storage
    URLs). If `fixity` is a "md5:<hex>" string, verify the download against it
    and raise on mismatch.

    Shows a byte progress bar (labelled `desc`) so a large full-dump download
    isn't mistaken for a stall; the bar auto-disables when not attached to a
    terminal (e.g. cron), and `quiet` forces it off (e.g. when the caller is
    logging to a file instead).
    """
    algo = expected = None
    hasher = None
    if fixity and ":" in fixity:
        algo, _, expected = fixity.partition(":")
        if algo == "md5":
            hasher = hashlib.md5(usedforsecurity=False)

    # No read timeout: full dumps are large and slow to stream.
    timeout = httpx.Timeout(60.0, read=None)
    with (
        path.open("wb") as output,
        httpx.stream(
            "GET", url, headers=_headers(), timeout=timeout, follow_redirects=True
        ) as resp,
    ):
        resp.raise_for_status()
        total = int(resp.headers.get("content-length") or 0) or None
        with tqdm(
            total=total,
            desc=desc or path.name,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            disable=True if quiet else None,
        ) as bar:
            for chunk in resp.iter_bytes():
                output.write(chunk)
                bar.update(len(chunk))
                if hasher is not None:
                    hasher.update(chunk)

    if hasher is not None and hasher.hexdigest() != expected:
        raise ValueError(
            f"fixity mismatch for {url}: expected {expected}, got {hasher.hexdigest()}"
        )

    return path


def read_delete_ids(path: Path) -> list[str]:
    """
    Read a ResourceSync deletes file (one bare record id per line).
    """
    ids = []
    with path.open() as fh:
        for line in fh:
            record_id = line.strip()
            if record_id:
                ids.append(record_id)
    return ids


def get_xml(url: str) -> etree._Element:
    resp = httpx.get(url, headers=_headers(), timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return etree.fromstring(resp.content)


def _headers() -> dict[str, str]:
    token = os.environ.get("PODBUCKET_POD_TOKEN")
    if token is None:
        raise RuntimeError("PODBUCKET_POD_TOKEN env var isn't set!")
    return {"Authorization": f"Bearer {token}"}
