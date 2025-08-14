import os
import logging
import time
import json
import dataclasses
from typing import Optional

from faker import Faker
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException, KafkaError
from confluent_kafka import Producer, Message
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RawLog:
    log_level: str
    message: str
    service_name: str

    def to_dict(self, ctx: SerializationContext):
        return dataclasses.asdict(self)

    def to_key_dict(self, ctx: SerializationContext):
        return {"service_name": self.service_name}

    @classmethod
    def auto(cls, faker: Faker):
        log_level = faker.random_element(elements=("INFO", "WARN", "ERROR", "CRITICAL"))
        service_name = faker.random_element(
            elements=("payment-service", "inventory-db", "shipping-api")
        )
        return cls(
            log_level=log_level,
            message=faker.sentence(),
            service_name=service_name,
        )

    @staticmethod
    def value_schema_str():
        return json.dumps(
            {
                "namespace": "io.factorhouse.demo",
                "type": "record",
                "name": "RawLog",
                "fields": [
                    {"name": "log_level", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "service_name", "type": "string"},
                ],
            },
            indent=2,
        )

    @staticmethod
    def key_schema_str():
        return json.dumps(
            {
                "namespace": "io.factorhouse.demo",
                "type": "record",
                "name": "RawLogKey",
                "fields": [{"name": "service_name", "type": "string"}],
            },
            indent=2,
        )


def create_topic(
    conf: dict,
    topic_name: str,
    num_partitions: int = 3,
    replication_factor: int = 3,
):
    admin_client = AdminClient(conf)
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)
    result_dict = admin_client.create_topics([new_topic])
    for topic, future in result_dict.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.warning(f"Topic '{topic_name}' already exists.")
            else:
                raise RuntimeError(f"Failed to create topic '{topic}'.") from e


def on_delivery(err: Optional[KafkaError], metadata: Message):
    if err is not None:
        logger.error(f"Error sending record: {err}")
    else:
        logger.info(
            f"Sent to {metadata.topic()} into partition {metadata.partition()}, offset {metadata.offset()}"
        )


if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "app-logs")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    logger.info(f"Producing to topic '{TOPIC_NAME}'")

    # --- Configuration Dictionaries ---
    client_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    registry_conf = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": "admin:admin",
    }

    # --- Initialize Clients and Serializers ---
    schema_registry_client = SchemaRegistryClient(registry_conf)

    key_serializer = AvroSerializer(
        schema_registry_client,
        RawLog.key_schema_str(),
        to_dict=lambda obj, ctx: obj.to_key_dict(ctx),
    )
    value_serializer = AvroSerializer(
        schema_registry_client,
        RawLog.value_schema_str(),
        to_dict=lambda obj, ctx: obj.to_dict(ctx),
    )

    # --- Topic Creation and Producer Loop ---
    create_topic(client_conf, TOPIC_NAME)

    faker = Faker()
    producer = Producer(client_conf)
    logger.info("Starting producer loop... Press Ctrl+C to stop.")

    try:
        while True:
            log = RawLog.auto(faker)
            producer.produce(
                topic=TOPIC_NAME,
                key=key_serializer(
                    log, SerializationContext(TOPIC_NAME, MessageField.KEY)
                ),
                value=value_serializer(
                    log, SerializationContext(TOPIC_NAME, MessageField.VALUE)
                ),
                on_delivery=on_delivery,
            )
            producer.poll(0)
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    except KafkaException as e:
        logger.error(f"Kafka error while producing: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error during production: {e}")
    finally:
        logger.info("Flushing final messages...")
        producer.flush()
        logger.info("Producer shut down.")
