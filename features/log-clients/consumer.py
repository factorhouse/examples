import os
import logging
from typing import List

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroDeserializer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def on_assign(consumer: Consumer, partitions: List[TopicPartition]):
    for p in partitions:
        logger.info(f"Assigned to {p.topic}, partiton {p.partition}")


if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "app-logs")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    logger.info(f"Consuming from topic '{TOPIC_NAME}'")

    client_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"{TOPIC_NAME}-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    registry_conf = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": "admin:admin",
    }
    schema_registry_client = SchemaRegistryClient(registry_conf)
    avro_descrializer = AvroDeserializer(
        schema_registry_client=schema_registry_client, from_dict=None
    )

    consumer = Consumer(client_conf)
    consumer.subscribe([TOPIC_NAME], on_assign=on_assign)
    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = avro_descrializer(
                    event.value(), SerializationContext(TOPIC_NAME, MessageField.VALUE)
                )
                if val is not None:
                    logger.info(f"Received: {val}")
                else:
                    logger.warning("Deserialized value is None")
                consumer.commit()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        logger.info("Closing consumer.")
        consumer.close()
