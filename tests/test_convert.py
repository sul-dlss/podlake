import pandas

from podlake.convert import oai_to_parquet


def test_convert(tmp_path):
    parquet_path = tmp_path / "test.parquet"
    oai_to_parquet("stanford", parquet_path=parquet_path, limit=2000)
    assert parquet_path.is_file()

    df = pandas.read_parquet(parquet_path)
    assert len(df) == 2000

    assert df["pod_record_id"].iloc[0] == "stanford:a1"
    assert df["F245"].iloc[0] == "Symphony, op. 38"

    assert df["pod_record_id"].iloc[1] == "stanford:a10"
    assert df["F245"].iloc[1] == "Panisci fistula; tre preludi per tre flauti."
