"""Abstract AWS Client."""
from abc import ABC

import boto3
from attrs import define, field


@define
class BaseAwsClient(ABC):
    """Abstract AWS Client for initial."""

    session: boto3.session = field(kw_only=True)

    def get_current_aws_identity(self) -> str:
        """
        Can be used to get current AWS account and IAM principal.

        :return:
        """
        try:
            session = self.session
            sts = session.client("sts")
            return str(sts.get_caller_identity())
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"error getting current aws caller identity {e}"
            ) from e
