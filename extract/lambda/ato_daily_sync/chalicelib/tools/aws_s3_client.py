"""AWS S3 Client base on BaseAwsClient - simply s3 client from session inherited."""
import gzip
import io
import json
import logging
from typing import Any

import boto3
from attrs import Factory, define, field

from ..models import S3UploadResponse  # noqa: TID252
from .base_aws_client import BaseAwsClient

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s - %(asctime)s - %(message)s"
)


@define
class AwsS3Client(BaseAwsClient):
    """AWS S3 Client base on BaseAwsClient - simply s3 client from session inherited."""

    s3_client: boto3.client = field(
        default=Factory(lambda self: self.session.client("s3"), takes_self=True),
        kw_only=True,
    )

    def get_bucket_acl(self, bucket_name: str) -> str:
        """
        Can be used to get an access control list (ACL) of an AWS S3 bucket.

        :param bucket_name: The bucket name that contains the object for which to get the ACL information.
        :return:
        """
        try:
            acl = self.s3_client.get_bucket_acl(Bucket=bucket_name)
            return str(acl)
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error getting bucket acl {e}"
            ) from e

    def get_bucket_policy(self, bucket_name: str) -> str:
        """
        Can be used to get an AWS S3 bucket policy.

        :param bucket_name: The bucket name for which to get the bucket policy.
        :return:
        """
        try:
            policy = self.s3_client.get_bucket_policy(Bucket=bucket_name)
            return str(policy)
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error getting bucket policy {e}"
            ) from e

    def get_object_acl(self, bucket_name: str, object_key: str) -> str:
        """
        Can be used to get an access control list (ACL) of an object in the AWS S3 bucket.

        :param bucket_name: Name of the AWS S3 bucket for which to get an ACL.
        :param object_key: Key of the object for which to get the ACL information.
        :return:
        """
        try:
            acl = self.s3_client.get_object_acl(Bucket=bucket_name, Key=object_key)
            return str(acl)
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error getting object acl {e}"
            ) from e

    def list_s3_buckets(self) -> list:
        """
        Can be used to list all AWS S3 buckets.

        :return: list of buckets that can be seen
        """
        try:
            buckets = self.s3_client.list_buckets()

            return [str(b) for b in buckets["Buckets"]]
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error listing s3 buckets {e}"
            ) from e

    def list_objects(self, bucket_name: str) -> list:
        """
        Can be used to list all objects in an AWS S3 bucket.

        :param bucket_name: The name of the S3 bucket to list.
        :return:
        """
        try:
            objects = self.s3_client.list_objects_v2(Bucket=bucket_name)

            return [str(o) for o in objects["Contents"]]
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error listing objects in bucket {e}"
            ) from e

    def upload_string_content_to_s3(
        self,
        content: str,
        bucket_name: str,
        object_key: str,
        extra_args: dict | None = None,
    ) -> None:
        """
        Can be used to upload content to an AWS S3 bucket.

        :param content:
        :param bucket_name:
        :param object_key: Destination object key name. For example, 'baz.txt'
        :param extra_args: Extra arguments that may be passed to the client operation. For allowed upload arguments see boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS.
        :return:
        """
        try:
            self._upload_object(bucket_name, object_key, content, extra_args)
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error uploading string objects to the bucket {e}"
            ) from e

    def upload_and_compress_gzip_file_to_s3(
        self,
        byte_value: io.BytesIO,
        bucket_name: str,
        prefix: str,
        file_name: str,
        extra_args: dict | None = None,
    ) -> S3UploadResponse:
        """
        Can be used to upload content to an AWS S3 bucket, compress in GZ format.

        :param byte_value: byte value of object want to upload
        :param bucket_name: Destination bucket name
        :param prefix: prefix of object key name. For example, 'ABC/DEF'
        :param file_name: Destination file name. For example, 'baz.txt'
        :param extra_args: Extra arguments that may be passed to the client operation. For allowed upload arguments see boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS.
        :return:
        """
        try:
            upload_object = gzip.compress(byte_value.getvalue())
            object_key = f"{prefix}/{file_name}"
            self._upload_object(bucket_name, object_key, upload_object, extra_args)
            return S3UploadResponse(
                file_name=file_name,
                s3_destination_bucket=bucket_name,
                s3_destination_key=object_key,
            )

        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error uploading objects to the bucket {e}"
            ) from e

    def upload_file_to_s3(
        self,
        byte_value: io.BytesIO,
        bucket_name: str,
        prefix: str,
        file_name: str,
        extra_args: dict | None = None,
    ) -> S3UploadResponse:
        """
        Can be used to upload content to an AWS S3 bucket, non compress.

        :param byte_value: byte value of object want to upload
        :param bucket_name: Destination bucket name
        :param prefix: prefix of object key name. For example, 'ABC/DEF'
        :param file_name: Destination file name. For example, 'baz.txt'
        :param extra_args: Extra arguments that may be passed to the client operation. For allowed upload arguments see boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS.
        :return:
        """
        try:
            object_key = f"{prefix}/{file_name}"
            self._upload_object(bucket_name, object_key, byte_value, extra_args)
            return S3UploadResponse(
                file_name=file_name,
                s3_destination_bucket=bucket_name,
                s3_destination_key=object_key,
            )
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error uploading objects to the bucket {e}"
            ) from e

    def construct_and_upload_manifest(
        self,
        detail_files_info: list[S3UploadResponse],
        bucket_name: str,
        prefix: str,
        file_name: str,
        extra_args: dict | None,
    ) -> S3UploadResponse:
        """
        Can be used to construct manifest file and push to S3 for further consumption.

        :param detail_files_info: list of S3UploadResponse
        :param bucket_name: Destination bucket name
        :param prefix: prefix of object key name. For example, 'ABC/DEF'
        :param file_name: Destination file name. For example, 'baz.txt'
        :param extra_args: Extra arguments that may be passed to the client operation. For allowed upload arguments see boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS.
        :return:
        """
        try:
            entries = [
                {
                    "url": f"s3://{file_info.s3_destination_bucket}/{file_info.s3_destination_key}",
                    "mandatory": True,
                }
                for file_info in detail_files_info
            ]
            upload_object = json.dumps({"entries": entries})
            object_key = f"{prefix}/{file_name}"
            self._upload_object(bucket_name, object_key, upload_object, extra_args)
            res = S3UploadResponse(
                file_name=file_name,
                s3_destination_bucket=bucket_name,
                s3_destination_key=object_key,
            )
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error uploading manifest objects to the bucket {e}"
            ) from e
        else:
            logging.info(
                f"SUCCESSFUL WRITE MANIFEST FILE TO "
                f"s3://{res.s3_destination_bucket}/{res.s3_destination_key}"
            )
            return res

    def _upload_object(
        self, bucket_name: str, object_name: str, value: Any, extra_args: dict | None
    ) -> None:
        self.s3_client.upload_fileobj(
            Fileobj=io.BytesIO(value.encode() if isinstance(value, str) else value),
            Bucket=bucket_name,
            Key=object_name,
            ExtraArgs=extra_args,
        )
        logging.info(f"SUCCESSFUL WRITE FILE TO s3://{bucket_name}/{object_name}")
