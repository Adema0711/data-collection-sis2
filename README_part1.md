# Часть участника 1: API и Producer

## Запуск
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d            # поднять Kafka Broker (localhost:9092)
cd src
python api_client.py            # сбор данных -> data/raw_data.csv, data/raw_data.json
python producer.py              # создание топика + отправка сообщений
```

## Ожидаемый результат
- `data/raw_data.csv` и `data/raw_data.json`: 500 записей (исходные данные)
- В консоли: `Успешно опубликовано: 500`

## Проверка, что сообщения в топике
```bash
docker exec taginfo-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic taginfo_keys
```
(в конце строки будет число сообщений)

## Настройки
Переменные окружения: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `TOTAL_RECORDS`, `PAGE_SIZE`.
