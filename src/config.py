"""Shared settings for the pipeline. Can be overridden with environment variables."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# --- World Bank API ---
API_BASE_URL = "https://api.worldbank.org/v2"
INDICATOR = "SP.POP.TOTL"  # total population
COUNTRIES = ["KAZ", "USA", "CHN", "IND", "BRA", "RUS", "DEU", "JPN", "NGA", "EGY"]
DATE_RANGE = "2000:2024"
PER_PAGE = 500
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# --- Raw data files (saved before any cleaning) ---
RAW_CSV = DATA_DIR / "raw_data.csv"
RAW_JSON = DATA_DIR / "raw_data.json"

# --- Kafka ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "worldbank_population")
KAFKA_PARTITIONS = 1
KAFKA_REPLICATION = 1
