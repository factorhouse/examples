import time
import json
import os
import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
POISON_COUNT = int(os.getenv("POISON_COUNT", 2000))

# Shared configuration
conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}

## Topic creation
print(f"Checking/Creating topic '{TOPIC}' with 6 partitions...")
admin_client = AdminClient(conf)

# Create a NewTopic object: Name, Partitions (6), Replication Factor (1)
new_topic = NewTopic(TOPIC, num_partitions=6, replication_factor=1)

# Call create_topics (returns a dict of futures)
fs = admin_client.create_topics([new_topic])

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        # If the topic already exists, this error will be raised. We catch it and proceed.
        print(f"Topic creation result: {e}")

## Producer logic
p = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


print(f"Starting Producer to {TOPIC}...")

message_count = 0
poison_pill_sent = False

try:
    while True:
        # Standard "Good" Message
        order_id = f"ORD-{message_count}"
        data = {
            "order_id": order_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": "NEW",
        }

        # Send to random partition (letting Kafka decide)
        p.produce(TOPIC, key=order_id, value=json.dumps(data), callback=delivery_report)

        # Inject Poison Pill (Once, after a few messages)
        # We target Partition 2 specifically (valid now that we have 6 partitions)
        if message_count == POISON_COUNT and not poison_pill_sent:
            print("!!! INJECTING POISON PILL TO PARTITION 2 !!!")
            poison_data = {
                "order_id": "POISON-PILL",
                "amount": "ONE THOUSAND DOLLARS",
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
        if message_count % 200 == 0:
            print(f"Sent {message_count} messages...")

except KeyboardInterrupt:
    pass
finally:
    p.flush()
