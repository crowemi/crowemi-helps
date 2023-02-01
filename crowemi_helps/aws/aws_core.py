import boto3

class AwsCore():
    def __init__(self, type: str = None, region: str = None) -> None:
        self.resource = boto3.resource(type, region)
        self.client = boto3.client(type, region)