"""AWS utils."""
import boto3


def aws_get_assumed_role_credentials(account_number: int, role_name: str) -> dict:
    """
    Get credentials for assumed role.

    :param account_number: account number of the cross account of assumed role
    :param role_name: assumed account role name
    :return: dict of credentials detail
    """
    sts_connection = boto3.client("sts")

    assumed_role = sts_connection.assume_role(
        RoleArn=f"arn:aws:iam::{account_number}:role/{role_name}",
        RoleSessionName="cross_account_lambda",
    )

    return assumed_role["Credentials"]


def aws_init_assumed_session(account_number: int, role_name: str) -> boto3.Session:
    """
    create session of assumed role.

    :param account_number: account number of the cross account of assumed role
    :param role_name: assumed account role name
    @return: boto3 session
    """
    assumed_role_credentials = aws_get_assumed_role_credentials(
        account_number=account_number, role_name=role_name
    )

    session = boto3.Session(
        aws_access_key_id=assumed_role_credentials["AccessKeyId"],
        aws_secret_access_key=assumed_role_credentials["SecretAccessKey"],
        aws_session_token=assumed_role_credentials["SessionToken"],
    )
    return session
