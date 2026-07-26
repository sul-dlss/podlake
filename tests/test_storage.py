import os

import boto3
import moto
import pytest

from podlake.storage import Storage

test_bucket_name = "test-bucket"


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3(aws_credentials):
    with moto.mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def test_bucket(s3):
    s3.create_bucket(Bucket=test_bucket_name)
    yield
    boto3.resource("s3").Bucket(test_bucket_name).objects.all().delete()
    s3.delete_bucket(Bucket=test_bucket_name)


def test_upload_file(tmp_path, test_bucket, s3):
    catalog = tmp_path / "podlake.ducklake"
    catalog.write_bytes(b"catalog")

    storage = Storage(f"s3://{test_bucket_name}/pod")
    storage.upload_file(catalog, "podlake.ducklake")

    obj = s3.get_object(Bucket=test_bucket_name, Key="pod/podlake.ducklake")
    assert obj["Body"].read() == b"catalog"


def test_sync_dir_preserves_structure(tmp_path, test_bucket, s3):
    records = tmp_path / "lake-data" / "main" / "records"
    records.mkdir(parents=True)
    (records / "a.parquet").write_bytes(b"A")
    (tmp_path / "lake-data" / "top.txt").write_bytes(b"T")

    storage = Storage(f"s3://{test_bucket_name}/pod")
    count = storage.sync_dir(tmp_path / "lake-data", "lake-data")

    assert count == 2
    keys = sorted(
        o["Key"] for o in s3.list_objects_v2(Bucket=test_bucket_name)["Contents"]
    )
    assert keys == [
        "pod/lake-data/main/records/a.parquet",
        "pod/lake-data/top.txt",
    ]


def test_sync_dir_missing_directory_is_noop(tmp_path, test_bucket):
    storage = Storage(f"s3://{test_bucket_name}")
    assert storage.sync_dir(tmp_path / "does-not-exist") == 0


def test_bad_uri_rejected():
    with pytest.raises(ValueError):
        Storage("not-an-s3-url")
