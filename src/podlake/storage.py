import logging
import os
import re
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, bucket_uri: str):
        self.bucket_name = re.sub(r"^s3://", "", bucket_uri)
        self.bucket = self._get_bucket()

    def has_file(self, path: Path) -> bool:
        key = self._key(path)

        try:
            self.bucket.Object(key).get()
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return False
            else:
                raise e

    def save_file(self, path: Path) -> None:
        key = self._key(path)
        logger.info(f"uploading {path} to {key}")
        self.bucket.upload_file(str(path), key)

    def _key(self, path: Path) -> str:
        # determining the org like this depends on it being the prefix
        # e.g. penn-2025-05-21-delta-marcxml.xml.gz
        org = path.name.split("-")[0]
        return f"org={org}/{path.name}"

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
            response = sts_client.assume_role(
                RoleArn=role, RoleSessionName="speech-to-text"
            )
            session = {
                "aws_access_key_id": response["Credentials"]["AccessKeyId"],
                "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
                "aws_session_token": response["Credentials"]["SessionToken"],
            }

        return session
