import pandas

from podbucket.convert import marcxml_to_parquet


def test_convert(tmp_path):
    marcxml_url = (
        "https://pod.stanford.edu/file/653722/stanford-2026-03-29T04-15-59-delta-marcxml.xml.gz"
    )

    parquet_path = marcxml_to_parquet(marcxml_url, output_dir=tmp_path)
    assert parquet_path.is_file()

    df = pandas.read_parquet(parquet_path)
    assert len(df) > 0
    assert df["F245"].iloc[0] == "25 études mélodiques et très faciles  : pour violon : précédées chacun d'un exercice préparatoire : op. 84 / par Charles Dancla."
