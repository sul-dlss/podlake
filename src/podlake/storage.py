import logging
import os
import re
from pathlib import Path

import boto3
from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource

logger = logging.getLogger(__name__)


class Storage:
    """
    A small helper for uploading files to an S3 bucket, used to publish a
    file-catalog DuckLake (the catalog file plus its Parquet data files) to a
    bucket that read-only consumers can attach to.
    """

    def __init__(self, bucket_uri: str):
        # accept s3://bucket or s3://bucket/some/prefix
        match = re.match(r"^s3://([^/]+)/?(.*)$", bucket_uri)
        if match is None:
            raise ValueError(f"expected an s3:// URL, got {bucket_uri!r}")
        self.bucket_name = match.group(1)
        self.prefix = match.group(2).strip("/")
        self.bucket = self._get_bucket()

    def upload_file(self, path: Path, key: str) -> None:
        full_key = self._full_key(key)
        logger.info(f"uploading {path} to s3://{self.bucket_name}/{full_key}")
        self.bucket.upload_file(str(path), full_key)

    def sync_dir(self, local_dir: Path, key_prefix: str = "") -> int:
        """
        Upload every file under local_dir to the bucket, preserving the
        directory structure under key_prefix. Returns the number of files
        uploaded.
        """
        if not local_dir.is_dir():
            return 0
        count = 0
        for path in sorted(local_dir.rglob("*")):
            if path.is_file():
                rel = path.relative_to(local_dir).as_posix()
                key = f"{key_prefix}/{rel}" if key_prefix else rel
                self.upload_file(path, key)
                count += 1
        return count

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def _get_bucket(self) -> Bucket:
        s3 = self._get_s3()
        return s3.Bucket(self.bucket_name)

    def _get_s3(self) -> S3ServiceResource:
        return boto3.resource("s3", **self._get_session())

    def _get_session(self) -> dict:
        # This would be a lot easier if boto3 read AWS_ROLE_ARN like it does other
        # environment variables:
        #
        # see: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html
        session = {}

        role = os.environ.get("AWS_ROLE_ARN")

        if role:
            sts_client = boto3.client("sts")
            response = sts_client.assume_role(RoleArn=role, RoleSessionName="podlake")
            session = {
                "aws_access_key_id": response["Credentials"]["AccessKeyId"],
                "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
                "aws_session_token": response["Credentials"]["SessionToken"],
            }

        return session
