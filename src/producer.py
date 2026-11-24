"""Kafka Producer: sends the collected records to a topic as JSON."""
import json
import logging
import sys

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from api_client import load_raw
from config import (KAFKA_BOOTSTRAP, KAFKA_PARTITIONS, KAFKA_REPLICATION,
                    KAFKA_TOPIC, RAW_JSON)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer")


def ensure_topic() -> None:
    """Check that the broker is reachable and create the topic if it is missing."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    try:
        metadata = admin.list_topics(timeout=10)
    except KafkaException as err:
        raise ConnectionError(f"Kafka broker unavailable ({KAFKA_BOOTSTRAP}): {err}")
    if KAFKA_TOPIC in metadata.topics:
        log.info("Topic '%s' already exists.", KAFKA_TOPIC)
        return
    futures = admin.create_topics([NewTopic(KAFKA_TOPIC,
                                            num_partitions=KAFKA_PARTITIONS,
                                            replication_factor=KAFKA_REPLICATION)])
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Topic '%s' created.", topic)
        except Exception as err:
            log.error("Could not create topic '%s': %s", topic, err)
            raise


def run_producer() -> dict:
    """Send every record and return delivery statistics."""
    records = load_raw()
    ensure_topic()

    stats = {"total": len(records), "delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            log.error("Delivery failed: %s", err)
        else:
            stats["delivered"] += 1

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})

    for record in records:
        message_id = f"{record.get('country_code')}_{record.get('year')}"
        try:
            value = json.dumps(record, ensure_ascii=False).encode("utf-8")  # JSON serialization
            key = message_id.encode("utf-8")
            producer.produce(KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        except (TypeError, ValueError) as err:
            stats["failed"] += 1
            log.error("Serialization error for %s: %s", message_id, err)
        except BufferError:
            producer.poll(1)  # local queue is full: wait, then retry
            producer.produce(KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        log.error("Messages not delivered after flush: %s", remaining)

    print(f"Source: {RAW_JSON}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Total records: {stats['total']}")
    print(f"Successfully published: {stats['delivered']}")
    print(f"Submission errors: {stats['failed']}")
    return stats


if __name__ == "__main__":
    try:
        result = run_producer()
    except (ConnectionError, FileNotFoundError) as err:
        log.error("%s", err)
        sys.exit(1)
    sys.exit(0 if result["failed"] == 0 else 2)
