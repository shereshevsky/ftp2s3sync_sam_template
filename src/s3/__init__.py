import boto3
from botocore.exceptions import ClientError
from typing import List, Dict
from loguru import logger
from structures import File

LOG = logger


class S3:
    def __init__(self, target_bucket):
        self.target_bucket = target_bucket
        self.s3 = boto3.client('s3')

    def check_state(self, target_path) -> List[File]:
        status_file = f"{target_path[1:]}/status.txt"
        try:
            status = self.s3.get_object(Bucket=self.target_bucket, Key=status_file)["Body"].read().decode()
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                LOG.info(f"Status file not found - {status_file}")
                return []
            else:
                raise ex
        return [File(i.split(',')[0], int(i.split(',')[1]), int(i.split(',')[2]), f"{target_path[1:]}/{i.split(',')[0]}")
                for i in status.split()]

    def head_object(self, file_path):
        head = self.s3.head_object(Bucket=self.target_bucket, Key=file_path)
        return head

    def create_multipart_upload(self, path):
        multipart = self.s3.create_multipart_upload(Bucket=self.target_bucket, Key=path)
        return multipart

    def complete_multipart_upload(self, key, upload_id, multipart):
        self.s3.complete_multipart_upload(
            Bucket=self.target_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload=multipart
        )

    def upload_part(self, key, part, upload_id, body):
        res = self.s3.upload_part(Bucket=self.target_bucket, Key=key, PartNumber=part, UploadId=upload_id, Body=body)
        return res
