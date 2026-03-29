from podbucket import resourcesync


def test_get_streams():
    streams = resourcesync.get_streams()
    assert len(streams) == 13
    assert (
        streams["stanford"]
        == "https://pod.stanford.edu/organizations/stanford/streams/2024-08-27/normalized_resourcelist/marcxml"
    )

    streams = resourcesync.get_streams("stanford")
    assert len(streams) == 1
    assert (
        streams["stanford"]
        == "https://pod.stanford.edu/organizations/stanford/streams/2024-08-27/normalized_resourcelist/marcxml"
    )


def test_get_resources():
    resources = resourcesync.get_resources(
        "https://pod.stanford.edu/organizations/stanford/streams/2024-08-27/normalized_resourcelist/marcxml"
    )
    assert len(resources) > 0
    assert resources[0].url
    assert resources[0].mediatype
    assert resources[0].length
    assert resources[0].fixity


def test_download(tmp_path):
    test_file = resourcesync.download(
        "https://pod.stanford.edu/file/653722/stanford-2026-03-29T04-15-59-delta-marcxml.xml.gz",
        tmp_path / "test.xml.gz",
    )
    assert test_file.is_file()
    assert test_file.stat().st_size == 1416574
