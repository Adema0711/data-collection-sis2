# Part 1: API Collection and Kafka Producer

This part of the pipeline pulls the most-used OpenStreetMap keys from the Taginfo API, saves the raw data, and publishes every record to a Kafka topic as JSON.

## Prerequisites
- Python 3.10+
- Docker Desktop (running)

## Quick start

Run all commands from the project root (`sis2/`), one per line.

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate           # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the Kafka broker (available at localhost:9092)
docker compose up -d

# 4. Collect data and publish it
cd src
python api_client.py             # saves data/raw_data.csv and data/raw_data.json
python producer.py               # creates the topic and sends the messages
```

**Windows PowerShell note:** if activation fails with "running scripts is disabled", run this once in the same window and try again:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Give the broker 10-15 seconds to start after `docker compose up -d` before running the producer.

## Expected results
- `data/raw_data.csv` and `data/raw_data.json` contain 500 raw
