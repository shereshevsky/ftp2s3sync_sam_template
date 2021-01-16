#! /usr/bin/python3

import math
import time
import shutil
import asyncio
import sentry_sdk
import watchtower, logging
from datetime import datetime
from typing import Dict, AnyStr

from s3 import S3
from ftp import FTP, ConnectionError, OldFTP
from ssm import SSM
from settings import *
from structures import File

sentry_sdk.init(DEFAULT_SENTRY, traces_sample_rate=1.0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ftp2s3")
logger.addHandler(watchtower.CloudWatchLogHandler())

logger.info("cold start")

ssm = SSM()


async def handler(event: Dict, context: Dict) -> None:
    """
    Handle all the ftp sources found in the AWS SSM secure parameter store and save the data into the target S3 bucket.
    Each source stored with the S3 prefix identical to SSM parameter prefix.
    Will work with all the parameters found in the DEFAULT_NAMESPACE variable.
    :param event:
    :param context:
    :return:
    """
    parameters = ssm.list_namespace(DEFAULT_NAMESPACE)

    logger.info(f"Handling data sources: {DEFAULT_NAMESPACE}")

    execution_time = datetime.utcnow().isoformat()

    for parameter in parameters:
        logger.info(f"Starting {parameter.get('s3_path')}")
        target = parameter.get("s3_path")[1:]  # remove leading slash
        source = parameter.get("connection_parameters")
        s3 = S3(source.get("bucket"), logger=logger, execution_time=execution_time)
        await process_data_source(s3, source, target)


async def process_data_source(s3, source: Dict, target: AnyStr) -> None:
    """
    Process single data source (sFTP server)
    :param s3: s3 handler
    :param source:  source parameters dictionary (host, port, user, ....)
    :param target:  target S3 path
    :return:
    """
    start = time.time()

    if source.get("type") == "sftp":
        try:
            ftp = FTP(logger=logger, **source)
        except ConnectionError:
            return
    else:
        try:
            ftp = OldFTP(logger=logger, **source)
        except ConnectionError:
            return

    ftp_files = ftp.list_dir(source.get("ftp_dir"))

    logger.info(f"All ftp files - {ftp_files}")
    current_s3_files = s3.check_state(target)
    logger.info(f"Files on S3 - {current_s3_files}")

    files_to_sync = []
    for file in ftp_files:
        if file not in current_s3_files:
            files_to_sync.append(file)
            logger.info(f"New file - {file.name}")
        else:
            if file.size > current_s3_files[current_s3_files.index(file)].size \
                    or file.mdate > current_s3_files[current_s3_files.index(file)].mdate:
                files_to_sync.append(file)
                logger.info(f"File with changed timestamp/size - {file.name}")
            else:
                logger.info(f"File identical to existing - {file.name}")

    for file in files_to_sync:
        await sync_file(s3, file, ftp, target)

    s3.log_synced_files(target, files_to_sync)
    s3.save_state(target, ftp_files)

    logger.info(f"Finished processing in {round(time.time() - start)} seconds.")


async def sync_file(s3, file: File, ftp: FTP, target: AnyStr) -> None:
    """
    Sync single file
    :param s3: S3 handler
    :param file: File object
    :param ftp: FTP connection
    :param target: target S3 path
    :return:
    """
    start = time.time()

    await clean_temp()

    ftp_file = ftp.read_file(file.path)
    chunk_count = int(math.ceil(file.size / float(DEFAULT_CHUNK_SIZE)))
    uploaded_subfiles = s3.decode_and_upload(chunk_count or 1, file, ftp_file, target)
    logger.info(f"Uploaded subfiles: {uploaded_subfiles}")
    logger.info(f'Finished syncing {file.name} in {round(time.time() - start)} seconds')
    ftp_file.close()
    s3.save_subfiles_status(target, uploaded_subfiles)


async def clean_temp():
    try:
        shutil.rmtree(TMP_PREFIX, ignore_errors=True)
    except OSError as e:
        logger.exception(e)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handler({}, {}))
    loop.close()
