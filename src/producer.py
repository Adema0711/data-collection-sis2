"""Kafka Producer: отправляет собранные записи Taginfo в топик в формате JSON."""
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
    """Проверяет доступность брокера и создаёт топик, если его нет."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    try:
        metadata = admin.list_topics(timeout=10)  # ошибка, если брокер недоступен
    except KafkaException as err:
        raise ConnectionError(f"Брокер Kafka недоступен ({KAFKA_BOOTSTRAP}): {err}")
    if KAFKA_TOPIC in metadata.topics:
        log.info("Топик '%s' уже существует.", KAFKA_TOPIC)
        return
    futures = admin.create_topics([NewTopic(KAFKA_TOPIC,
                                            num_partitions=KAFKA_PARTITIONS,
                                            replication_factor=KAFKA_REPLICATION)])
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Топик '%s' создан.", topic)
        except Exception as err:
            log.error("Не удалось создать топик '%s': %s", topic, err)
            raise


def run_producer() -> dict:
    """Отправляет все записи и возвращает статистику отправки."""
    records = load_raw()
    ensure_topic()

    stats = {"total": len(records), "delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            log.error("Ошибка доставки: %s", err)
        else:
            stats["delivered"] += 1

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})

    for record in records:
        try:
            value = json.dumps(record, ensure_ascii=False).encode("utf-8")  # JSON-сериализация
            key = str(record.get("key", "")).encode("utf-8")
            producer.produce(KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        except (TypeError, ValueError) as err:
            stats["failed"] += 1
            log.error("Ошибка сериализации записи %s: %s", record.get("key"), err)
        except BufferError:
            producer.poll(1)  # очередь переполнена: ждём и отправляем дальше
            producer.produce(KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        log.error("Не доставлено сообщений после flush: %s", remaining)

    print(f"Источник: {RAW_JSON}")
    print(f"Топик: {KAFKA_TOPIC}")
    print(f"Всего записей: {stats['total']}")
    print(f"Успешно опубликовано: {stats['delivered']}")
    print(f"Ошибок отправки: {stats['failed']}")
    return stats


if __name__ == "__main__":
    try:
        result = run_producer()
    except (ConnectionError, FileNotFoundError) as err:
        log.error("%s", err)
        sys.exit(1)
    sys.exit(0 if result["failed"] == 0 else 2)
