import time
import json
import os
import random
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
POISON_COUNT = int(os.getenv("POISON_COUNT", 50))

p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


print(f"Starting Producer to {TOPIC}...")

message_count = 0
poison_pill_sent = False

try:
    while True:
        # 1. Standard "Good" Message
        order_id = f"ORD-{message_count}"
        data = {
            "order_id": order_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": "NEW",
        }

        # Send to random partition (letting Kafka decide)
        p.produce(TOPIC, key=order_id, value=json.dumps(data), callback=delivery_report)

        # 2. Inject Poison Pill (Once, after a few messages)
        # We target Partition 2 specifically
        if message_count == POISON_COUNT and not poison_pill_sent:
            print("!!! INJECTING POISON PILL TO PARTITION 2 !!!")
            poison_data = {
                "order_id": "POISON-PILL",
                "amount": "ONE THOUSAND DOLLARS",  # String instead of Float -> Schema Mismatch
                "status": "CORRUPT",
            }
            # Explicitly force partition 2
            p.produce(
                TOPIC,
                key="POISON",
                value=json.dumps(poison_data),
                partition=2,
                callback=delivery_report,
            )
            poison_pill_sent = True

        p.poll(0)
        message_count += 1

        # Speed: 10 messages per second
        time.sleep(0.1)

        if message_count % 10 == 0:
            p.flush()

except KeyboardInterrupt:
    pass
finally:
    p.flush()
