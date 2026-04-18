import io
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
def sts(aws_credentials):
    with moto.mock_aws():
        yield boto3.client("sts")


@pytest.fixture
def s3(aws_credentials):
    with moto.mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def test_bucket(s3, sts):
    s3.create_bucket(Bucket=test_bucket_name)

    yield

    # clear all the objects from the bucket and remove the bucket
    boto3.resource("s3").Bucket(test_bucket_name).objects.all().delete()
    s3.delete_bucket(Bucket="test-bucket")


def test_upload_file(tmp_path, test_bucket, s3):
    # create a test file
    test_file = tmp_path / "stanford-2025-05-26-delta-marcxml.parquet"

    # create our storage object pointed at the bucket
    storage = Storage(f"s3://{test_bucket_name}")

    assert storage.has_file(test_file) is False, "file isn't in the bucket"

    # write some data to the file and save it to storage
    test_file.open("w").write("000")
    storage.save_file(test_file)

    assert storage.has_file(test_file) is True, "file was uploaded to bucket"

    # ensure that the file was written where we expect it to be
    bucket = boto3.resource("s3").Bucket(test_bucket_name)
    key = f"org=stanford/{test_file.name}"
    fh = io.BytesIO()
    bucket.download_fileobj(key, fh)
    fh.seek(0)
    assert fh.read() == b"000", "content was written to the file"
