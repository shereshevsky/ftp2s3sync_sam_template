# settings.py
import os
from dotenv import load_dotenv

load_dotenv()

DEFAULT_NAMESPACE = os.getenv("DEFAULT_NAMESPACE")
DEFAULT_CHUNK_SIZE = int(os.getenv("DEFAULT_CHUNK_SIZE", 6291456))
DEFAULT_SENTRY = os.getenv("DEFAULT_SENTRY")
