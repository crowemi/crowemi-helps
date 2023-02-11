import os
import boto3


class AwsCore:
    def __init__(
        self,
        type: str = None,
        region: str = None,
        endpoint_url: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ) -> None:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", None)
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
        if aws_access_key_id and aws_secret_access_key:
            self.resource = boto3.resource(
                service_name=type,
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            self.client = boto3.client(
                service_name=type,
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            self.resource = boto3.resource(
                service_name=type,
                region_name=region,
            )
            self.client = boto3.client(
                service_name=type,
                region_name=region,
            )
