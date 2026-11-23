
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Taginfo API
API_URL = "https://taginfo.openstreetmap.org/api/4/keys/all"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 100))      
TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", 500)) 
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

RAW_CSV = DATA_DIR / "raw_data.csv"
RAW_JSON = DATA_DIR / "raw_data.json"

#Kafka 
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taginfo_keys")
KAFKA_PARTITIONS = 1
KAFKA_REPLICATION = 1
