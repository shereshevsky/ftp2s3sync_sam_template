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

    def create_multipart_upload(self, name, target):
        multipart = self.s3.create_multipart_upload(Bucket=self.target_bucket, Key=f"{target}/{name}")
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

    def upload_multichunk_file(self, chunk_count, file, ftp_file, target):
        multipart_handler = self.s3.create_multipart_upload(file.name, target)
        parts = [self.put_chunk(i, chunk_count, file, ftp_file, multipart_handler) for i in range(chunk_count)]
        parts_info = {'Parts': parts}
        self.s3.complete_multipart_upload(key=f"{target}/{file.name}",
                                     upload_id=multipart_handler['UploadId'], multipart=parts_info)

    def put_chunk(self, i, chunk_count, file, ftp_file, multipart_handler):
        LOG.debug(f"Transferring chunk {i + 1} / {chunk_count} of {file.name}")
        return self.copy_chunk_to_s3(ftp_file, file.path, multipart_handler, i + 1)

    def copy_chunk_to_s3(self, ftp_file, file_path, multipart_upload, part_number):
        chunk = ftp_file.read(DEFAULT_CHUNK_SIZE)
        part = self.s3.upload_part(file_path, part_number, multipart_upload['UploadId'], chunk)
        return {'PartNumber': part_number, 'ETag': part['ETag']}
