import sentry_sdk
import loguru
from s3 import S3
from ssm import SSM
import asyncio
from ftp import FTP
import math
import json
from settings import *

sentry_sdk.init(DEFAULT_SENTRY, traces_sample_rate=1.0)

LOG = loguru.logger

LOG.debug("cold start")

ssm = SSM()
s3 = S3(DEFAULT_BUCKET)


async def handler(event, context):
    LOG.debug(event)
    parameters = ssm.list_namespace(DEFAULT_NAMESPACE)

    for parameter in parameters:
        target = parameter.get("path")
        source = json.loads(parameter.get("connection_string"))
        await process_data_source(source, target)


async def process_data_source(source: dict, target: str):
    ftp = FTP(**source)
    ftp_files = ftp.list_dir(DEFAULT_FTP_DIR)
    print(ftp_files)
    current_s3_files = s3.check_state(target)
    print(current_s3_files)
    files_to_sync = []
    for file in ftp_files:
        if file not in current_s3_files:
            files_to_sync.append(file)
            LOG.debug(f"New file - {file.name}")
        else:
            if file.size > current_s3_files[current_s3_files.index(file)].size \
                    or file.mdate > current_s3_files[current_s3_files.index(file)].mdate:
                files_to_sync.append(file)
                LOG.debug(f"File with changed timestamp/size - {file.name}")
            else:
                LOG.debug(f"File identical to existing - {file.name}")

    for file in files_to_sync:
        ftp_file = ftp.read_file(file.path)
        chunk_count = int(math.ceil(file.size / float(DEFAULT_CHUNK_SIZE)))
        multipart_handler = s3.create_multipart_upload(file.path)
        parts = []
        for i in range(chunk_count):
            LOG.debug(f"Transferring chunk {i + 1} / {chunk_count} of {file.name}")
            part = copy_chunk_to_s3(ftp_file, file.path, multipart_handler, i + 1)
            parts.append(part)

        parts_info = {'Parts': parts}
        s3.complete_multipart_upload(key=file.path, upload_id=multipart_handler['UploadId'], multipart=parts_info)
        LOG.debug(f'Finished syncing {file.name}.')
        ftp_file.close()


def copy_chunk_to_s3(ftp_file, file_path, multipart_upload, part_number):
    chunk = ftp_file.read(DEFAULT_CHUNK_SIZE)
    part = s3.upload_part(file_path, part_number, multipart_upload['UploadId'], chunk)
    return {'PartNumber': part_number, 'ETag': part['ETag']}


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handler({}, {}))
    loop.close()
