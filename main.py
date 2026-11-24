import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from config import DATA_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC, RAW_JSON

STAGES = ["collect", "produce", "consume", "validate", "clean", "analyze", "load"]


def read_report(name):
    path = DATA_DIR / name
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def reset_topic(timeout=30):
    """Delete the topic (if it exists) so that every pipeline run starts with an empty topic.
    Without this, repeated producer runs pile up old messages and the counters differ."""
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    if KAFKA_TOPIC not in admin.list_topics(timeout=10).topics:
        return
    for future in admin.delete_topics([KAFKA_TOPIC]).values():
        future.result()
    deadline = time.monotonic() + timeout
    while KAFKA_TOPIC in admin.list_topics(timeout=10).topics:
        if time.monotonic() > deadline:
            raise TimeoutError(f"Topic '{KAFKA_TOPIC}' was not deleted in {timeout} s")
        time.sleep(1)
    print(f"Old topic '{KAFKA_TOPIC}' deleted, a new one will be created by the producer")


def run_stage(stage):
    if stage == "collect":
        import api_client
        records = api_client.collect_records()
        api_client.save_raw(records)
    elif stage == "produce":
        import producer
        reset_topic()
        stats = producer.run_producer()
        with open(DATA_DIR / "producer_report.json", "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
    elif stage == "consume":
        import consumer
        consumer.run_consumer()
    elif stage == "validate":
        import validator
        validator.run_validation()
    elif stage == "clean":
        import cleaner
        cleaner.run_cleaning()
    elif stage == "analyze":
        import analysis
        analysis.run_analysis()
    elif stage == "load":
        import loader
        loader.run_loader()


def add_row(rows, stage, count_in, count_out, reason):
    if count_in is None or count_out is None:
        rows.append([stage, count_in, count_out, "-", "stage was not run"])
        return
    diff = count_in - count_out
    rows.append([stage, count_in, count_out, diff, "OK" if diff == 0 else reason])


def build_table():
    raw = None
    if RAW_JSON.exists():
        with open(RAW_JSON, encoding="utf-8") as f:
            raw = len(json.load(f))
    prod = read_report("producer_report.json")
    cons = read_report("consumer_report.json")
    val = read_report("validation_report.json")
    clean = read_report("cleaning_report.json")
    load = read_report("load_report.json")

    topic = cons.get("expected")
    sent = prod.get("delivered")
    old_messages = topic - sent if topic is not None and sent is not None else 0

    rows = []
    add_row(rows, "API -> Kafka producer", raw, sent,
            f"{prod.get('failed')} messages were not sent")
    add_row(rows, "producer -> topic", sent, topic,
            f"{old_messages} old messages from previous producer runs are still in the topic")
    add_row(rows, "topic -> consumer", topic, cons.get("decoded"),
            f"decode errors: {cons.get('decode_errors')}, kafka errors: {cons.get('kafka_errors')}, "
            f"stop reason: {cons.get('stop_reason')}")
    add_row(rows, "consumer -> validation", cons.get("decoded"), val.get("received"),
            "validator read a different file")
    add_row(rows, "validation", val.get("received"), val.get("valid"),
            f"{val.get('invalid')} invalid records (duplicates or broken values), see invalid_data.csv")
    add_row(rows, "valid -> cleaning", val.get("valid"), clean.get("input_rows"),
            "cleaner read a different file")
    add_row(rows, "cleaning", clean.get("input_rows"), clean.get("output_rows"),
            f"removed: {clean.get('missing_rows')} with missing values, "
            f"{clean.get('invalid_numeric_rows')} with wrong numbers, "
            f"{clean.get('duplicate_rows')} duplicates")
    add_row(rows, "cleaning -> database", clean.get("output_rows"), load.get("rows_after"),
            f"{load.get('ignored')} rows were ignored as duplicates")
    add_row(rows, "second load", load.get("rows_after"), load.get("rows_after_second_load"),
            "second load added rows")
    if load.get("data_changed"):
        rows[-1][4] = "second load changed the data"

    lines = ["# Pipeline verification", "",
             "| Stage | In | Out | Difference | Comment |",
             "|---|---|---|---|---|"]
    for row in rows:
        lines.append("| " + " | ".join("-" if x is None else str(x) for x in row) + " |")
    if load.get("checks"):
        lines += ["", "| SQL check | Result |", "|---|---|"]
        for name, value in load["checks"].items():
            lines.append(f"| {name} | {value} |")

    with open(DATA_DIR / "pipeline_verification.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    start = sys.argv[1] if len(sys.argv) > 1 else "collect"
    if start not in STAGES:
        print(f"Unknown stage. Use one of: {', '.join(STAGES)}")
        return
    for stage in STAGES[STAGES.index(start):]:
        print(f"\n===== {stage} =====")
        try:
            run_stage(stage)
        except Exception as e:
            print(f"Stage {stage} failed: {e}")
            break
    print()
    build_table()


if __name__ == "__main__":
    main()