# settings.py
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

DEFAULT_NAMESPACE = os.getenv("DEFAULT_NAMESPACE")
DEFAULT_CHUNK_SIZE = int(os.getenv("DEFAULT_CHUNK_SIZE", 6291456))
DEFAULT_SENTRY = os.getenv("DEFAULT_SENTRY")
DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-central-1")
AWS_CONFIG = None
TMP_PREFIX = "/tmp/ftp2s3"

if not os.getenv("AWS_ACCESS_KEY_ID"):
    session = boto3.Session(DEFAULT_REGION)
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    AWS_CONFIG = credentials
