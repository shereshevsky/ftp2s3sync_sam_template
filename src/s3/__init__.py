from botocore.exceptions import ClientError
from typing import List, Dict
from loguru import logger
from pathlib import Path
from pyunpack import Archive

from structures import File
from settings import *

LOG = logger


class S3:
    def __init__(self, target_bucket):
        self.target_bucket = target_bucket
        self.s3 = boto3.client('s3', region_name=DEFAULT_REGION)

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

        tmp_path = Path("/tmp" + file.path)
        tmp_path.unlink(missing_ok=True)
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_out_path = tmp_path.with_suffix("")
        tmp_out_path.mkdir(parents=True, exist_ok=True)
        for i in range(chunk_count):
            self.save_chunk(i, chunk_count, file, ftp_file, tmp_path)

        archive = Archive(tmp_path)
        archive.extractall(tmp_out_path)
        for subfile in list(Path(tmp_path).glob("*")):
            key = f"{target}/{file.name}/{subfile.name}"
            multipart_handler = self.s3.create_multipart_upload(Bucket=self.target_bucket, Key=key)


            # parts_info = {'Parts': parts}
            # self.s3.complete_multipart_upload(
            #     Bucket=self.target_bucket, Key=f"{target}/{file.name}", UploadId=multipart_handler['UploadId'],
            #     MultipartUpload=parts_info)

    def save_chunk(self, i, chunk_count, file, ftp_file, tmp_path):
        LOG.debug(f"Transferring chunk {i + 1} / {chunk_count} of {file.name}")
        chunk = ftp_file.read(DEFAULT_CHUNK_SIZE)

        with open(tmp_path, "ba+") as f:
            f.write(chunk)


        # res = self.s3.upload_part(Bucket=self.target_bucket, Key=key, PartNumber=i + 1,
        #                           UploadId=multipart_handler['UploadId'], Body=chunk)
        # return {'PartNumber': i + 1, 'ETag': res['ETag']}
        return