# settings.py
import os
from dotenv import load_dotenv
load_dotenv()

DEFAULT_NAMESPACE = os.getenv("DEFAULT_NAMESPACE")
DEFAULT_BUCKET = os.getenv("DEFAULT_NAMESPACE")
DEFAULT_FTP_DIR = os.getenv("DEFAULT_FTP_DIR")
DEFAULT_CHUNK_SIZE = os.getenv("DEFAULT_CHUNK_SIZE", 6291456)
DEFAULT_SENTRY = os.getenv("DEFAULT_SENTRY")
