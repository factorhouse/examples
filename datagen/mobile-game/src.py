import json
import dataclasses
import logging
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException, KafkaError
from confluent_kafka import Producer, Message
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


@dataclasses.dataclass
class UserScore:
    """A dataclass representing a user score event, designed for Avro serialization."""

    user_id: str
    team_id: str
    team_name: str
    score: int
    event_time_millis: int
    readable_time: str
    event_type: str

    def to_dict(self, ctx: SerializationContext):
        return dataclasses.asdict(self)

    @staticmethod
    def schema_str():
        # fmt: off
        schema = {
            "namespace": "io.factorhouse.avro",
            "type": "record",
            "name": "UserScore",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "team_id", "type": "string"},
                {"name": "team_name", "type": "string"},
                {"name": "score", "type": "int"},
                {"name": "event_time_millis", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "readable_time", "type": "string"},
                {"name": "event_type", "type": "string"}
            ],
        }
        # fmt: on
        return json.dumps(schema, indent=2)


class Publisher:
    """Handles publishing events to a Kafka topic"""

    def __init__(
        self, bootstrap_servers: str, topic_name: str, schema_registry_url: str
    ):
        kafka_conf = {"bootstrap.servers": bootstrap_servers}
        registry_conf = {
            "url": schema_registry_url,
            "basic.auth.user.info": "admin:admin",
        }
        self.topic_name = topic_name
        self._create_topic_if_not_exists(kafka_conf, self.topic_name)
        self.avro_serializer = self._initialize_serializer(registry_conf)
        self.producer = Producer(kafka_conf)
        logging.info(f"Initialized Avro Kafka producer for topic '{self.topic_name}'")

    def _create_topic_if_not_exists(
        self,
        conf: dict,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ):
        """Uses an AdminClient to create a topic."""
        admin_client = AdminClient(conf)
        new_topic = NewTopic(topic_name, num_partitions, replication_factor)
        result_dict = admin_client.create_topics([new_topic])
        for topic, future in result_dict.items():
            try:
                future.result()
                logging.info(f"Topic '{topic}' created")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logging.warning(f"Topic '{topic_name}' already exists.")
                else:
                    raise RuntimeError(f"Failed to create topic '{topic}'.") from e

    def _initialize_serializer(self, registry_conf: dict) -> AvroSerializer:
        """Initializes and returns the AvroSerializer."""
        schema_registry_client = SchemaRegistryClient(registry_conf)
        return AvroSerializer(
            schema_registry_client,
            UserScore.schema_str(),
            lambda obj, ctx: obj.to_dict(ctx),
        )

    def publish(self, user_score: UserScore):
        """Publishes a single UserScore event."""
        self.producer.poll(0)
        self.producer.produce(
            topic=self.topic_name,
            key=user_score.user_id.encode("utf-8"),
            value=self.avro_serializer(
                user_score,
                SerializationContext(self.topic_name, MessageField.VALUE),
            ),
            on_delivery=self.on_delivery,
        )

    def close(self):
        """Closes resources, ensuring all messages are sent."""
        if self.producer:
            logging.info("Flushing final messages to Kafka...")
            self.producer.flush()
            logging.info("Kafka producer flushed.")

    @staticmethod
    def on_delivery(err: Optional[KafkaError], msg: Message):
        """Kafka delivery report callback."""
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )
