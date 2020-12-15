import csv
import pandas as pd
from io import BytesIO
from typing import List
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime
from botocore.exceptions import ClientError

from settings import *
from structures import File


class S3:
    def __init__(self, target_bucket, logger):
        self.target_bucket = target_bucket
        self.logger = logger
        self.s3 = boto3.client('s3', region_name=DEFAULT_REGION)

    def check_state(self, target_path) -> List[File]:
        status_file = f"{target_path}/status.txt"
        try:
            status = self.s3.get_object(Bucket=self.target_bucket, Key=status_file)["Body"].read().decode()
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                self.logger.info(f"Status file not found - {status_file}")
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

    def decode_and_upload(self, chunk_count, file, ftp_file, target):

        uploaded_files = []
        tmp_path = Path(TMP_PREFIX + file.path)
        tmp_path.unlink(missing_ok=True)
        tmp_path.parent.mkdir(parents=True, exist_ok=True)

        for i in range(chunk_count):
            self.save_chunk(i, chunk_count, file, ftp_file, tmp_path)

        tmp_out_path = tmp_path.with_suffix("")
        tmp_out_path.mkdir(parents=True, exist_ok=True)

        archive = ZipFile(open(tmp_path, "rb"))
        archive.extractall(tmp_out_path)

        for subfile in list(Path(tmp_out_path).glob("*.txt")) + list(Path(tmp_out_path).glob("*/*")):
            zip_folder = file.name.split('.')[0]

            self.logger.warning(f"Unploading target={target}, zip_folder={zip_folder}, subfile={subfile}")

            if subfile.name == "Geographies.txt" and zip_folder == "ImsDataKaz":
                df = pd.read_csv(subfile, delimiter=";", )
                dff = df[["GeographyID", "GeographyName", "ParentID"]]
                dff.columns = ["GeographyID","GeographyEnglish","ParentID"]
                dff.to_csv(subfile, index=False, quoting=csv.QUOTE_ALL, sep=";")

            key = f"{target}/{zip_folder}/{subfile.name}"
            multipart_handler = self.s3.create_multipart_upload(Bucket=self.target_bucket, Key=key)
            upload_id = multipart_handler['UploadId']
            binary_stream = BytesIO(subfile.read_bytes())

            multi_parts = []
            chunk = binary_stream.read(DEFAULT_CHUNK_SIZE).decode("cp1251")
            part_number = 1
            while chunk:
                res = self.s3.upload_part(Bucket=self.target_bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=chunk)
                multi_parts.append({'PartNumber': part_number, 'ETag': res['ETag']})
                part_number += 1
                chunk = binary_stream.read(DEFAULT_CHUNK_SIZE).decode("cp1251")

            parts_info = {'Parts': multi_parts}
            self.s3.complete_multipart_upload(
                Bucket=self.target_bucket, Key=key, UploadId=upload_id, MultipartUpload=parts_info)

            uploaded_files.append([str(subfile), datetime.utcnow().isoformat(), key])
        return uploaded_files

    def save_chunk(self, i, chunk_count, file, ftp_file, tmp_path):
        self.logger.info(f"Transferring chunk {i + 1} / {chunk_count} of {file.name}")
        chunk = ftp_file.read(DEFAULT_CHUNK_SIZE)

        with open(tmp_path, "ba+") as f:
            f.write(chunk)

        return

    def save_subfiles_status(self, target, uploaded_subfiles):
        self.s3.put_object(Bucket=self.target_bucket, Key=f"{target}/subfiles_status.txt",
                           Body="\n".join([",".join(i) for i in uploaded_subfiles]))
