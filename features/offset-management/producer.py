import os
import time
import datetime
import json
import logging

from confluent_kafka import Producer, Message, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


def topic_exists(admin_client: AdminClient, topic_name: str):
    ## check if a topic exists
    metadata = admin_client.list_topics()
    for value in iter(metadata.topics.values()):
        logging.info(value.topic)
        if value.topic == topic_name:
            return True
    return False


def create_topic(admin_client: AdminClient, topic_name: str):
    ## create a new topic
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    result_dict = admin_client.create_topics([new_topic])
    for topic, future in result_dict.items():
        try:
            future.result()
            logging.info(f"Topic {topic} created")
        except Exception as e:
            logging.info(f"Failed to create topic {topic}: {e}")


def callback(err: KafkaError, event: Message):
    ## callback function that gets triggered when a message is delivered
    if err:
        logging.info(
            f"Produce to topic {event.topic()} failed for event: {event.key()}"
        )
    else:
        value = event.value().decode("utf8")
        logging.info(
            f"Sent: {value} to partition {event.partition()}, offset {event.offset()}."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "offset-management")

    ## create a topic
    conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf)
    if not topic_exists(admin_client, TOPIC_NAME):
        create_topic(admin_client, TOPIC_NAME)

    ## producer messages
    producer = Producer(conf)
    for _ in range(10):
        dt = datetime.datetime.now()
        epoch = int(dt.timestamp() * 1000)
        ts = dt.isoformat(timespec="seconds")
        producer.produce(
            topic=TOPIC_NAME,
            value=json.dumps({"epoch": epoch, "ts": ts}).encode("utf-8"),
            key=str(epoch),
            on_delivery=callback,
        )
        producer.flush()
        time.sleep(0.2)
