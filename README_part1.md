# Part 1: API Collection and Kafka Producer

This part of the pipeline takes the most-used OpenStreetMap keys from the Taginfo API, saves the raw data, and sends every record to a Kafka topic as JSON.

## Prerequisites
- Python 3.10+
- Docker Desktop (running)

## API description

- **Source:** [OpenStreetMap Taginfo](https://taginfo.openstreetmap.org/taginfo/apidoc), a public service with usage statistics for OSM tags. No API key is needed.
- **Endpoint:** `GET https://taginfo.openstreetmap.org/api/4/keys/all`
- **Parameters:** `page` (page number), `rp=100` (results per page), `sortname=count_all` and `sortorder=desc` (most used keys first).
- **How data is collected:** `api_client.py` requests pages one by one until it has 500 records. Each record gets three extra fields: `rank`, `data_until` and `collected_at`. The raw data is then saved to `data/raw_data.csv` and `data/raw_data.json`, before any cleaning.
- **Error handling:** every request has a 30 s timeout and is retried up to 3 times if the network fails or the server returns a bad status.
- **Fields:** `key` (text, unique), numeric counts and shares (`count_all`, `count_nodes`, `count_ways`, `count_relations`, `*_fraction`, `values_all`, `users_all`, `projects`, `rank`), `in_wiki` (boolean) and two date/time fields (`data_until`, `collected_at`).

## Quick start

Run everything from the project root (`sis2/`).

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate           

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the Kafka broker (available at localhost:9092)
docker compose up -d

# 4. Collect data and publish it
cd src
python api_client.py       
python producer.py            
```

**Windows PowerShell:** if activation fails with "running scripts is disabled", run this once in the same window and try again:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```


## Expected results
- `data/raw_data.csv` and `data/raw_data.json` contain 500 raw records.
- `api_client.py` prints `All records received: 500`.
- `producer.py` prints `Successfully published: 500` and `Submission errors: 0`.

## Checking the messages in Kafka

```bash
docker exec taginfo-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic taginfo_keys
```

The output `taginfo_keys:0:500` means partition 0 holds 500 messages.
