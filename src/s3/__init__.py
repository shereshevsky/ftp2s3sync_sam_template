import boto3
from botocore.exceptions import ClientError
from typing import List, Dict
from loguru import logger
from structures import File
from settings import *

LOG = logger


class S3:
    def __init__(self, target_bucket):
        self.target_bucket = target_bucket
        # if AWS_CONFIG:
        #     self.s3 = boto3.client('s3', config=AWS_CONFIG)
        # else:
        self.s3 = boto3.client('s3')

    def check_state(self, target_path) -> List[File]:
        status_file = f"{target_path}/status.txt"
        try:
            status = self.s3.get_object(Bucket=self.target_bucket, Key=status_file)["Body"].read().decode()
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                LOG.info(f"Status file not found - {status_file}")
                return []
            else:
                raise ex
        return [File(i.split(',')[0], int(i.split(',')[1]), int(i.split(',')[2]), f"{target_path}/{i.split(',')[0]}")
                for i in status.split()]

    def save_state(self, target_path, ftp_files):
        self.s3.put_object(Bucket=self.target_bucket, Key=f"{target_path}/status.txt",
                           Body="\n".join([f"{i.name},{i.mdate},{i.size},{i.path}" for i in ftp_files])
                           )

    def head_object(self, file_path):
        head = self.s3.head_object(Bucket=self.target_bucket, Key=file_path)
        return head

    def complete_multipart_upload(self, key, upload_id, multipart):
        return self.s3.complete_multipart_upload(
            Bucket=self.target_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload=multipart
        )

    def upload_multichunk_file(self, chunk_count, file, ftp_file, target):
        key = f"{target}/{file.name}"
        multipart_handler = self.s3.create_multipart_upload(Bucket=self.target_bucket, Key=key)
        parts = [self.put_chunk(i, key, chunk_count, file, ftp_file, multipart_handler) for i in range(chunk_count)]
        parts_info = {'Parts': parts}
        self.s3.complete_multipart_upload(
            Bucket=self.target_bucket, Key=f"{target}/{file.name}", UploadId=multipart_handler['UploadId'],
            MultipartUpload=parts_info)

    def put_chunk(self, i, key,  chunk_count, file, ftp_file, multipart_handler):
        LOG.debug(f"Transferring chunk {i + 1} / {chunk_count} of {file.name}")
        chunk = ftp_file.read(DEFAULT_CHUNK_SIZE)
        res = self.s3.upload_part(Bucket=self.target_bucket, Key=key, PartNumber=i + 1,
                                  UploadId=multipart_handler['UploadId'], Body=chunk)
        return {'PartNumber': i + 1, 'ETag': res['ETag']}
