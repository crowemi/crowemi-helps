import gzip
import shutil
import boto3

from crowemi_helps.aws.aws_core import AwsCore

class AwsS3(AwsCore):
    def __init__(self) -> None:
        super().__init__()

    def write_s3(self, key: str, bucket: str, content: str, compress: bool = False):
        """ Method for writing content to S3.
        ---
        Keyword arguments: \n
        key -- the S3 resource key. \n
        content -- the content to be written to S3. \n
        compress -- flag to indicate whether to compress content.
        """

        if compress:
            content = gzip.compress(bytes(content, 'utf-8'))

        return self.aws_client.put_object(Body=content, Bucket=bucket, Key=key)

    def copy_object(self, destination_bucket: str, destination_key: str, source_bucket_key: str, storage_class: str = 'STANDARD'):
        """ Method for copying S3 objects from Glacier storage.
        ---
        Keyword arguments: \n
        destination_bucket: the destination bucket for the newly created object. \n
        destination_key: the destination key for the newly created object. \n
        source_bucket_key: the full key including the bucket of the object to be copied. \n
        storage_class: the storage class for the newly created object. \n
        """
        return self.aws_client.copy_object(
            Bucket=destination_bucket,
            CopySource=source_bucket_key,
            Key=destination_key,
            StorageClass=storage_class
        )

    def restore_object(self, bucket: str, key: str, restore_days: int = 7, restore_tier: str = 'STANDARD'):
        """ Method for restoring S3 objects from Glacier storage.
        ---
        Keyword arguments: \n
        bucket: the restore objet S3 bucket.
        key: the restore object S3 key.
        restore_days: the number of days the restore will be available.
        restore_tier: the Glacier restore tier, how quickly the restore occurs.
        """
        ret = None
        try:
            ret = self.aws_client.restore_object(
                Bucket=bucket, 
                Key=key, 
                RestoreRequest={'Days': restore_days, 'GlacierJobParameters': {'Tier': restore_tier}}
            )
        except self.ClientError as error:
            ret = error.response
            if ret['Error'].get('Code') == "RestoreAlreadyInProgress":
                print(f"Restore already in progress for {key}")
            else:
                raise error

        return ret

    def delete_s3(self, key: str, bucket: str):
        """ Method for deleting content from S3.
        ---
        Keyword arguments: \n
        key -- the S3 resource key.
        """
        return self.aws_client.delete_object(Bucket=bucket, Key=key)

    def upload_s3(self, file_to_upload: str, bucket: str, key: str, compress: bool = False):
        if compress:
            with open(file_to_upload, 'rb') as file_in:
                with gzip.open(f'{file_to_upload}.gz', 'wb') as file_out:
                    shutil.copyfileobj(file_in, file_out)
                    file_to_upload = f'{file_to_upload}.gz'

        return self.aws_client.upload_file(file_to_upload, bucket, key)

    def get_object(self, key: str, bucket: str):
        return self.aws_client.get_object(Bucket=bucket, Key=key)

    def list_object(self, key: str, bucket: str):
        return self.aws_client.list_objects_v2(Bucket=bucket, Prefix=key)

    def list_objects(self, prefix: str, bucket: str, next_token: str = None):
        ret = None
        if next_token:
            ret = self.aws_client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=next_token)
        else:
            ret = self.aws_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        return ret

    def object_exists(self, key: str, bucket: str):
        ret = self.aws_client.list_objects_v2(Bucket=bucket, Prefix=key)
        return True if ret['KeyCount'] > 0 else False