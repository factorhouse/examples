import os
import logging
import time
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException, KafkaError
from confluent_kafka import Producer, Message
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from model import OrderV1, OrderV2

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


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
    TOPIC_NAME = os.getenv("TOPIC_NAME", "orders-clnt")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    try:
        MODEL_VERSION = int(os.getenv("MODEL_VERSION", "1"))
        max_records_env = os.getenv("MAX_RECORDS")
        MAX_RECORDS = (
            int(max_records_env)
            if max_records_env and int(max_records_env) > 0
            else None
        )
    except (ValueError, TypeError):
        MODEL_VERSION = 1
        MAX_RECORDS = None

    logger.info(
        f"bootstrap: {BOOTSTRAP_SERVERS}, topic: {TOPIC_NAME}, model: {MODEL_VERSION}"
    )

    client_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    registry_conf = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": "admin:admin",
    }
    schema_registry_client = SchemaRegistryClient(registry_conf)
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=OrderV1.schema_str() if MODEL_VERSION == 1 else OrderV2.schema_str(),
        to_dict=lambda obj, ctx: obj.to_dict(ctx),
    )

    ## create a topic if not existing
    create_topic(client_conf, TOPIC_NAME)

    num_rec = 0
    producer = Producer(client_conf)
    while True:
        try:
            order = OrderV1.create() if MODEL_VERSION == 1 else OrderV2.create()
            producer.produce(
                topic=TOPIC_NAME,
                key=order.order_id,
                value=avro_serializer(
                    order, SerializationContext(TOPIC_NAME, MessageField.VALUE)
                ),
                on_delivery=on_delivery,
            )
            producer.flush()
            num_rec += 1
            if MAX_RECORDS is not None and num_rec >= MAX_RECORDS:
                logger.info(
                    f"Max records ({MAX_RECORDS}) reached. Stop producing messages."
                )
                break
            time.sleep(1)
        except KafkaException as e:
            logger.error(f"Kafka error while producing: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during production: {e}")
