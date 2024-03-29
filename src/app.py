import sentry_sdk
import loguru
import asyncio
import math
import time
from typing import Dict, AnyStr
from settings import *

from ftp import FTP
from s3 import S3
from ssm import SSM


sentry_sdk.init(DEFAULT_SENTRY, traces_sample_rate=1.0)

LOG = loguru.logger

LOG.debug("cold start")

ssm = SSM()


async def handler(event, context):
    parameters = ssm.list_namespace(DEFAULT_NAMESPACE)

    LOG.debug(f"Handling data sources: {parameters}")

    for parameter in parameters:
        LOG.debug(f"Starting {parameter}")
        if 'connection_parameters' not in parameter or 'bucket' not in parameter['connection_parameters']:
            continue
        target = parameter.get("s3_path")[1:]  # remove leading slash
        source = parameter.get("connection_parameters")
        s3 = S3(source.get("bucket"))
        await process_data_source(s3, source, target)


async def process_data_source(s3, source: Dict, target: AnyStr):
    start = time.time()
    ftp = FTP(**source)
    ftp_files = ftp.list_dir(source.get("ftp_dir"))
    LOG.debug(f"All ftp files - {ftp_files}")
    current_s3_files = s3.check_state(target)
    LOG.debug(f"Files on S3 - {current_s3_files}")

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
        await sync_file(s3, file, ftp, target)

    s3.save_state(target, ftp_files)

    LOG.debug(f"Finished processing {source} in {round(time.time() - start)} seconds.")


async def sync_file(s3, file, ftp, target):
    start = time.time()
    ftp_file = ftp.read_file(file.path)
    chunk_count = int(math.ceil(file.size / float(DEFAULT_CHUNK_SIZE)))
    if chunk_count:
        s3.upload_multichunk_file(chunk_count, file, ftp_file, target)
    LOG.debug(f'Finished syncing {file.name} in {round(time.time() - start)} seconds')
    ftp_file.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handler({}, {}))
    loop.close()
