import argparse
import logging

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class KafkaTopicManager:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str):
        kafka_conf = {"bootstrap.servers": bootstrap_servers}
        registry_conf = {
            "url": schema_registry_url,
            "basic.auth.user.info": "admin:admin",
        }
        self.admin_client = AdminClient(kafka_conf)
        self.schema_registry_client = SchemaRegistryClient(registry_conf)

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ):
        """Creates a Kafka topic if it doesn't already exist."""
        new_topic = NewTopic(topic_name, num_partitions, replication_factor)
        result_dict = self.admin_client.create_topics([new_topic])

        for topic, future in result_dict.items():
            try:
                future.result()
                logging.info(f"Topic '{topic}' created.")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logging.warning(f"Topic '{topic}' already exists.")
                else:
                    raise RuntimeError(f"Failed to create topic '{topic}'.") from e

    def delete_topic(self, topic_name: str):
        """Deletes a Kafka topic."""
        result_dict = self.admin_client.delete_topics([topic_name])

        for topic, future in result_dict.items():
            try:
                future.result()
                logging.info(f"Topic '{topic}' deleted successfully.")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logging.warning(f"Topic '{topic}' does not exist.")
                else:
                    raise RuntimeError(f"Failed to delete topic '{topic}'.") from e

    def delete_value_schema(self, topic_name: str):
        """Deletes the value schema subject for the given topic."""
        subject = f"{topic_name}-value"

        try:
            deleted_versions = self.schema_registry_client.delete_subject(subject)
            if deleted_versions:
                logging.info(
                    f"Deleted schema versions {deleted_versions} for subject '{subject}'."
                )
            else:
                logging.warning(f"No versions found for subject '{subject}'.")
        except SchemaRegistryError as e:
            if e.http_status_code == 404:
                logging.warning(f"Schema subject '{subject}' not found.")
            else:
                raise RuntimeError(
                    f"Failed to delete schema for subject '{subject}'."
                ) from e


def main():
    # fmt: off
    parser = argparse.ArgumentParser(description="Kafka topic management")
    parser.add_argument("--action", choices=["create", "delete"], required=True, help="Action to perform on the topics")
    parser.add_argument(
        "--topic-names", type=str, default="top-teams,top-players,hot-streakers,team-mvps",
        help="Comma-separated Kafka topic names Defaults to: top-teams,top-players,hot-streakers,team-mvps",
    )
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server addresses.")
    parser.add_argument("--schema-registry-url", type=str, default="http://localhost:8081", help="Schema registry URL.")
    parser.add_argument("--topic-name", type=str, default="user-score", help="Kafka topic name")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    # fmt: on

    args = parser.parse_args()
    topic_names = [t.strip() for t in args.topic_names.split(",") if t.strip()]
    manager = KafkaTopicManager(args.bootstrap_servers, args.schema_registry_url)

    if args.action == "create":
        for topic in topic_names:
            manager.create_topic(topic, args.partitions, args.replication_factor)
    elif args.action == "delete":
        for topic in topic_names:
            manager.delete_topic(topic)
            manager.delete_value_schema(topic)


if __name__ == "__main__":
    main()
