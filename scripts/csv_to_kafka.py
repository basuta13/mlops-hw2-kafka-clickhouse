import argparse
import csv
import json
import sys
from datetime import datetime
from decimal import Decimal

from confluent_kafka import Producer


COLUMNS = (
    "transaction_time merch cat_id amount name_1 name_2 gender "
    "street one_city us_state post_code lat lon population_city "
    "jobs merchant_lat merchant_lon target"
).split()

FLOAT_COLUMNS = ("lat", "lon", "merchant_lat", "merchant_lon")


def read_records(path):
    with open(path, encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, strict=True)
        headers = reader.fieldnames or []

        if len(headers) != len(COLUMNS) or set(headers) != set(COLUMNS):
            raise ValueError(f"Unexpected CSV columns: {headers}")

        for row_id, row in enumerate(reader, start=1):
            try:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError("Incorrect number of CSV fields")

                row["row_id"] = row_id
                row["transaction_time"] = datetime.strptime(
                    row["transaction_time"], "%Y-%m-%d %H:%M"
                ).strftime("%Y-%m-%d %H:%M:%S")

                amount = Decimal(row["amount"])
                if (
                    not amount.is_finite()
                    or abs(amount) >= Decimal("1e16")
                    or amount != amount.quantize(Decimal("0.01"))
                ):
                    raise ValueError("Amount does not fit Decimal(18, 2)")

                row["amount"] = format(amount, ".2f")

                for column in FLOAT_COLUMNS:
                    row[column] = float(row[column])

                row["population_city"] = int(row["population_city"])
                row["target"] = int(row["target"])

                if not 0 <= row["population_city"] <= 4294967295:
                    raise ValueError("Population does not fit UInt32")
                if row["target"] not in (0, 1):
                    raise ValueError("Target must be 0 or 1")

                payload = json.dumps(
                    row,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode("utf-8")

            except Exception as exc:
                raise ValueError(f"CSV record {row_id}: {exc}") from exc

            yield row_id, payload


def main():
    parser = argparse.ArgumentParser(description="Load CSV into Kafka")
    parser.add_argument("--csv", default="data/train.csv")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    args = parser.parse_args()

    print("Validating CSV...", flush=True)
    expected = sum(1 for _ in read_records(args.csv))
    if expected == 0:
        raise ValueError("CSV contains no records")
    print(f"Validated: {expected} records", flush=True)

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "csv-loader",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
        "linger.ms": 20,
        "batch.size": 131072,
        "queue.buffering.max.messages": 50000,
        "delivery.timeout.ms": 120000,
    })

    metadata = producer.list_topics(timeout=15)
    topic = metadata.topics.get(args.topic)
    if topic is None or topic.error is not None:
        raise RuntimeError(f"Kafka topic is unavailable: {args.topic}")

    delivered = 0
    errors = []

    def on_delivery(error, message):
        nonlocal delivered
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    for row_id, payload in read_records(args.csv):
        while True:
            if errors:
                raise RuntimeError(f"Kafka delivery failed: {errors[0]}")
            try:
                producer.produce(
                    args.topic,
                    key=str(row_id).encode("utf-8"),
                    value=payload,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.5)

        producer.poll(0)

        if row_id % 100000 == 0:
            print(f"Queued: {row_id}/{expected}", flush=True)

    remaining = producer.flush(120)

    if remaining or errors or delivered != expected:
        raise RuntimeError(
            f"Incomplete delivery: expected={expected}, "
            f"delivered={delivered}, pending={remaining}, "
            f"first_error={errors[:1]}"
        )

    print(f"Delivered to Kafka: {delivered} records", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
