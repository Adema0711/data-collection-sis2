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

# --- Part 2: Kafka Consumer and Pandera validation ---
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "worldbank_consumer_group")
CONSUMER_IDLE_TIMEOUT = 15        # seconds without messages before the consumer stops
CONSUMER_STARTUP_TIMEOUT = 30     # seconds to wait for the broker / partition assignment

CONSUMED_JSON = DATA_DIR / "consumed_data.json"             # all messages received from Kafka
VALID_CSV = DATA_DIR / "valid_data.csv"                     # records that passed validation (input for cleaner.py)
INVALID_CSV = DATA_DIR / "invalid_data.csv"                 # records that failed validation + reasons
TEST_VALIDATION_CSV = DATA_DIR / "validation_test_results.csv"  # deliberately broken test records (never used downstream)
VALIDATION_REPORT_JSON = DATA_DIR / "validation_report.json"    # counters of the validation stage
CONSUMER_REPORT_JSON = DATA_DIR / "consumer_report.json"        # counters of the consumer stage
