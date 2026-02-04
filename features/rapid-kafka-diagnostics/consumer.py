import json
import os
import time
from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
GROUP_ID = os.getenv("GROUP_ID", "orders-fulfillment")

c = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        # CRITICAL: We disable auto-commit to simulate "stuck on offset"
        "enable.auto.commit": False,
    }
)

c.subscribe([TOPIC])

print(f"Consumer started. Group: {GROUP_ID}. Waiting for assignment...")

try:
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Simulate Processing
        try:
            payload = msg.value().decode("utf-8")
            data = json.loads(payload)

            # --- THE TRAP: SCHEMA VALIDATION ---
            # We expect 'amount' to be a float.
            # If it's a string, we simulate a strict logic failure.
            if not isinstance(data["amount"], (int, float)):
                raise ValueError(
                    f"Schema Mismatch! Amount must be number, got {type(data['amount'])}"
                )

            # If successful:
            print(
                f"Partition {msg.partition()} | Offset {msg.offset()} | Processed: {data['order_id']}"
            )

            # Manually commit success
            c.commit(message=msg)

        except ValueError as e:
            # --- THE ZOMBIE LOGIC ---
            print(
                f"!!! CRITICAL ERROR on Partition {msg.partition()} Offset {msg.offset()} !!!"
            )
            print(f"Reason: {e}")
            print("Retrying in 2 seconds... (Infinite Loop)")

            # 1. We do NOT commit.
            # 2. We seek back to the CURRENT offset to re-read it next poll
            #    (Note: In a simpler loop we could just `continue` without commit,
            #     but explicit seek ensures we verify the 'stuck' behavior).
            tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
            c.seek(tp)

            # 3. We sleep, but we continue the loop to call poll() again.
            #    This sends Heartbeats! The broker thinks we are healthy.
            time.sleep(2)
except KeyboardInterrupt:
    pass
finally:
    c.close()
