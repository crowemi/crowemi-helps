import boto3

from aws.aws_core import AwsCore

class AwsS3(AwsCore):
    def __init__(self) -> None:
        super().__init__()

    def get_object_content(self, bucket: str, key: str) -> str:
        obj = self.resource.Object(bucket, key)
        return obj.get()['Body'].read().decode('utf-8')

    def get_object(self, bucket: str, key: str):
        return self.client.get_object(Bucket=bucket, Key=key)

    def list_object(self, bucket: str, key: str):
        return self.client.list_objects_v2(Bucket=bucket, Prefix=key)

    def list_objects(self, prefix: str, bucket: str, next_token: str = None):
        ret = None
        if next_token:
            ret = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=next_token)
        else:
            ret = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return ret

    def object_exists(self,bucket: str, key: str):
        ret = self.client.list_objects_v2(Bucket=bucket, Prefix=key)
        return True if ret['KeyCount'] > 0 else False