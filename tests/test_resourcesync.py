from lxml import etree

from podlake import resourcesync

INDEX_XML = b"""<sitemapindex
  xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
  xmlns:rs="http://www.openarchives.org/rs/terms/">
  <sitemap><loc>https://pod.stanford.edu/organizations/brown/streams/brown_2022-05-05/normalized_resourcelist/marcxml</loc></sitemap>
  <sitemap><loc>https://pod.stanford.edu/organizations/stanford/streams/2024-08-27/normalized_resourcelist/marcxml</loc></sitemap>
</sitemapindex>"""

RESOURCELIST_XML = b"""<urlset
  xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
  xmlns:rs="http://www.openarchives.org/rs/terms/">
  <url>
    <loc>https://pod.stanford.edu/file/1/brown-2026-02-11-full-marcxml.xml.gz</loc>
    <lastmod>2026-02-11T01:50:10Z</lastmod>
    <rs:md type="application/gzip" length="100" hash="md5:aaa"/>
  </url>
  <url>
    <loc>https://pod.stanford.edu/file/2/brown-2026-02-11T00-00-01-delta-marcxml.xml.gz</loc>
    <lastmod>2026-02-11T00:00:04Z</lastmod>
    <rs:md type="application/gzip" length="50" hash="md5:bbb"/>
  </url>
  <url>
    <loc>https://pod.stanford.edu/file/3/brown-2026-02-11T00-00-01-delta-deletes.del.txt</loc>
    <lastmod>2026-02-11T00:00:05Z</lastmod>
    <rs:md type="text/plain" length="19" hash="md5:ccc"/>
  </url>
</urlset>"""


def test_get_streams(monkeypatch):
    monkeypatch.setattr(
        resourcesync, "get_xml", lambda url: etree.fromstring(INDEX_XML)
    )
    assert set(resourcesync.get_streams()) == {"brown", "stanford"}
    # name filter, case-insensitive
    assert set(resourcesync.get_streams("brown")) == {"brown"}
    assert set(resourcesync.get_streams("BROWN")) == {"brown"}


def test_get_resources_sorted_and_classified(monkeypatch):
    monkeypatch.setattr(
        resourcesync, "get_xml", lambda url: etree.fromstring(RESOURCELIST_XML)
    )
    resources = resourcesync.get_resources("https://example.test/resourcelist")

    # returned oldest-first by lastmod: delta 00:00:04, deletes 00:00:05, full 01:50:10
    assert [r.kind for r in resources] == ["delta", "deletes", "full"]
    assert resources[0].mediatype == "application/gzip"
    assert resources[0].length == 50
    assert resources[0].fixity == "md5:bbb"
    assert resources[0].lastmod < resources[-1].lastmod


def test_read_delete_ids(tmp_path):
    path = tmp_path / "x.del.txt"
    path.write_text("991044000187606966\n\n123\n")
    assert resourcesync.read_delete_ids(path) == ["991044000187606966", "123"]
