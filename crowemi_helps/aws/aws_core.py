import boto3

class AwsCore():
    def __init__(self, type: str = None, region: str = None) -> None:
        # TODO: configure options
        self.resource = boto3.resource('s3')
        self.client = boto3.client('s3')