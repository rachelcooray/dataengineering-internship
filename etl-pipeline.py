import os
import re
import json
import time
from datetime import datetime, date

import pandas as pd
import requests

CLICKSTREAM_PATH = "clickstream.csv"
TRANSACTIONS_PATH = "transactions.csv"
API_KEY = "b74bf2ded174e3515ea87712"  
API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"

CHUNK_SIZE = 50_000
LOCAL_PROCESSED_DIR = "data/processed"
RAW_API_DIR = "data/raw/api_currency"
INGEST_DATE = date.today().strftime("%Y-%m-%d")

