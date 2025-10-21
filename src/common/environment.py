import os
from datetime import datetime


class Environment:
    API_USERNAME = os.getenv("API_USERNAME", "")
    API_KEY = os.getenv("API_KEY", "")
    UPDATE_COUNTRIES = os.getenv("UPDATE_COUNTRIES", "False").lower() == "true"
    START_YEAR = int(os.getenv("START_YEAR", 2010))
    END_YEAR = int(os.getenv("END_YEAR", datetime.now().year))
    CHECKPOINT = os.getenv("CHECKPOINT", "True").lower() == "true"
    BUCKET_NAME = os.getenv("BUCKET_NAME", "bucket")
