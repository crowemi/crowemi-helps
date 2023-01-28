import boto3

resource = boto3.resource('s3')
client = boto3.client('s3')

def get_object_content(bucket: str, key: str) -> str:
    obj = resource.Object(bucket, key)
    return obj.get()['Body'].read().decode('utf-8')

def get_object(bucket: str, key: str):
    return client.get_object(Bucket=bucket, Key=key)

def list_object(bucket: str, key: str):
    return client.list_objects_v2(Bucket=bucket, Prefix=key)

def list_objects(prefix: str, bucket: str, next_token: str = None):
    ret = None
    if next_token:
        ret = client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=next_token)
    else:
        ret = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return ret

def object_exists(bucket: str, key: str):
    ret = client.list_objects_v2(Bucket=bucket, Prefix=key)
    return True if ret['KeyCount'] > 0 else False