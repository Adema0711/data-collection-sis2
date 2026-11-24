"""Kafka Consumer: reads the records from the topic, deserializes JSON and saves them."""
import json
import logging
import sys
import time

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from config import (CONSUMED_JSON, CONSUMER_IDLE_TIMEOUT, CONSUMER_REPORT_JSON,
                    CONSUMER_STARTUP_TIMEOUT, DATA_DIR, KAFKA_BOOTSTRAP,
                    KAFKA_GROUP_ID, KAFKA_TOPIC)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")


def count_messages_in_topic(consumer: Consumer) -> int:
    """Number of messages currently stored in the topic (sum over all partitions)."""
    try:
        metadata = consumer.list_topics(KAFKA_TOPIC, timeout=CONSUMER_STARTUP_TIMEOUT)
    except KafkaException as err:
        raise ConnectionError(f"Kafka broker unavailable ({KAFKA_BOOTSTRAP}): {err}")
    topic_meta = metadata.topics.get(KAFKA_TOPIC)
    if topic_meta is None or topic_meta.error is not None:
        raise LookupError(f"Topic '{KAFKA_TOPIC}' does not exist. Run producer.py first.")
    total = 0
    for partition_id in topic_meta.partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(KAFKA_TOPIC, partition_id), timeout=10)
        total += high - low
    return total


def read_from_beginning(consumer: Consumer, partitions: list) -> None:
    """Every run reads the whole topic from the first message (the stage is repeatable)."""
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


def run_consumer() -> dict:
    """Consume the topic until all messages are read (or the stream is idle) and save them."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    stats = {"expected": 0, "consumed": 0, "decoded": 0, "decode_errors": 0,
             "kafka_errors": 0, "stop_reason": ""}
    records: list[dict] = []

    try:
        stats["expected"] = count_messages_in_topic(consumer)
        log.info("Messages in topic '%s': %s", KAFKA_TOPIC, stats["expected"])
        consumer.subscribe([KAFKA_TOPIC], on_assign=lambda c, parts: read_from_beginning(c, parts))

        last_message_time = time.monotonic()
        while True:
            # Termination condition 1: everything that was in the topic has been read
            if stats["consumed"] >= stats["expected"]:
                stats["stop_reason"] = "all messages consumed"
                break
            # Termination condition 2: no new messages for a long time (safety stop)
            if time.monotonic() - last_message_time > CONSUMER_IDLE_TIMEOUT:
                stats["stop_reason"] = f"idle for {CONSUMER_IDLE_TIMEOUT} s"
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                stats["kafka_errors"] += 1
                log.error("Kafka error: %s", msg.error())
                continue

            last_message_time = time.monotonic()
            stats["consumed"] += 1
            try:
                record = json.loads(msg.value().decode("utf-8"))  # JSON deserialization
                if not isinstance(record, dict):
                    raise ValueError("message is not a JSON object")
                records.append(record)
                stats["decoded"] += 1
            except (UnicodeDecodeError, ValueError) as err:   # JSONDecodeError is a ValueError
                stats["decode_errors"] += 1
                log.error("Cannot decode message (partition %s, offset %s): %s",
                          msg.partition(), msg.offset(), err)
    finally:
        consumer.close()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONSUMED_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    with open(CONSUMER_REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Messages in topic: {stats['expected']}")
    print(f"Successfully consumed: {stats['consumed']}")
    print(f"Successfully decoded (JSON): {stats['decoded']}")
    print(f"Decoding errors: {stats['decode_errors']}")
    print(f"Kafka errors: {stats['kafka_errors']}")
    print(f"Stopped because: {stats['stop_reason']}")
    print(f"Saved to: {CONSUMED_JSON}")
    return stats


if __name__ == "__main__":
    try:
        result = run_consumer()
    except (ConnectionError, LookupError) as err:
        log.error("%s", err)
        sys.exit(1)
    except KafkaException as err:
        log.error("Kafka failure: %s", err)
        sys.exit(1)
    sys.exit(0 if result["decode_errors"] == 0 and result["kafka_errors"] == 0 else 2)
