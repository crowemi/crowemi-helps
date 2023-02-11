import gzip
import shutil
import boto3

from crowemi_helps.aws.aws_core import AwsCore


class AwsS3(AwsCore):
    def __init__(
        self,
        region: str = "us-west-2",
        endpoint_url: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ) -> None:
        super().__init__(
            "s3",
            region,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key,
        )

    def write_s3(self, key: str, bucket: str, content: str, compress: bool = False):
        """Method for writing content to S3.
        ---
        Keyword arguments: \n
        key -- the S3 resource key. \n
        content -- the content to be written to S3. \n
        compress -- flag to indicate whether to compress content.
        """

        if compress:
            content = gzip.compress(bytes(content, "utf-8"))

        return self.client.put_object(Body=content, Bucket=bucket, Key=key)

    def copy_object(
        self,
        destination_bucket: str,
        destination_key: str,
        source_bucket_key: str,
        storage_class: str = "STANDARD",
    ):
        """Method for copying S3 objects from Glacier storage.
        ---
        Keyword arguments: \n
        destination_bucket: the destination bucket for the newly created object. \n
        destination_key: the destination key for the newly created object. \n
        source_bucket_key: the full key including the bucket of the object to be copied. \n
        storage_class: the storage class for the newly created object. \n
        """
        return self.client.copy_object(
            Bucket=destination_bucket,
            CopySource=source_bucket_key,
            Key=destination_key,
            StorageClass=storage_class,
        )

    def restore_object(
        self,
        bucket: str,
        key: str,
        restore_days: int = 7,
        restore_tier: str = "STANDARD",
    ):
        """Method for restoring S3 objects from Glacier storage.
        ---
        Keyword arguments: \n
        bucket: the restore objet S3 bucket.
        key: the restore object S3 key.
        restore_days: the number of days the restore will be available.
        restore_tier: the Glacier restore tier, how quickly the restore occurs.
        """
        ret = None
        try:
            ret = self.client.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest={
                    "Days": restore_days,
                    "GlacierJobParameters": {"Tier": restore_tier},
                },
            )
        except self.ClientError as error:
            ret = error.response
            if ret["Error"].get("Code") == "RestoreAlreadyInProgress":
                print(f"Restore already in progress for {key}")
            else:
                raise error

        return ret

    def delete_s3(self, key: str, bucket: str):
        """Method for deleting content from S3.
        ---
        Keyword arguments: \n
        key -- the S3 resource key.
        """
        return self.client.delete_object(Bucket=bucket, Key=key)

    def upload_s3(
        self, file_to_upload: str, bucket: str, key: str, compress: bool = False
    ):
        if compress:
            with open(file_to_upload, "rb") as file_in:
                with gzip.open(f"{file_to_upload}.gz", "wb") as file_out:
                    shutil.copyfileobj(file_in, file_out)
                    file_to_upload = f"{file_to_upload}.gz"

        return self.client.upload_file(file_to_upload, bucket, key)

    def get_object(self, key: str, bucket: str):
        return self.client.get_object(Bucket=bucket, Key=key)

    def get_object_content(self, bucket: str, key: str) -> str:
        obj = self.resource.Object(bucket, key)
        return obj.get()["Body"].read().decode("utf-8")

    def list_object(self, key: str, bucket: str):
        return self.client.list_objects_v2(Bucket=bucket, Prefix=key)

    def list_objects(
        self,
        prefix: str,
        bucket: str,
        next_token: str = None,
        limit: int = None,
    ):
        """Boto3 `list_objects` wrapper.\n
        ---
        prefix: \n
        bucket: \n
        next_token: \n
        limit: \n
        """
        ret = None
        if next_token:
            ret = self.client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=next_token
            )
        else:
            ret = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        return ret

    def object_exists(self, key: str, bucket: str):
        ret = self.client.list_objects_v2(Bucket=bucket, Prefix=key)
        return True if ret["KeyCount"] > 0 else False
