import asyncio
import logging
import os
from typing import Dict, List, Callable, Awaitable

import pandas as pd
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- Basic Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# --- Kafka and Schema Registry Configuration ---
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC_NAMES = ["top-teams", "top-players"]
CONSUMER_GROUP = "streamlit-consumer"


class RealtimeLeaderboardConsumer:
    """
    A single, self-contained class that manages a Kafka connection,
    deserializes messages, maintains leaderboard state, and runs the
    main processing loop.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topics: List[str],
        group_id: str,
    ):
        logger.info(f"Initializing consumer for group '{group_id}' on topics {topics}")

        # --- State Management ---
        self.teams: pd.DataFrame = pd.DataFrame(
            columns=["rnk", "team_id", "team_name", "total_score"]
        ).set_index("rnk")
        self.players: pd.DataFrame = pd.DataFrame(
            columns=["rnk", "user_id", "team_name", "total_score"]
        ).set_index("rnk")
        self.lock = asyncio.Lock()  # For safe state updates and reads

        # --- Kafka Consumer Setup ---
        client_conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
        }
        self._consumer = Consumer(client_conf)
        self._consumer.subscribe(topics)

        # --- Deserializer Setup ---
        registry_conf = {
            "url": schema_registry_url,
            "basic.auth.user.info": "admin:admin",
        }
        schema_registry_client = SchemaRegistryClient(registry_conf)
        self._avro_deserializer = AvroDeserializer(schema_registry_client)
        self._running = True

    async def _message_stream(self):
        """(Internal) An async generator that yields deserialized messages."""
        loop = asyncio.get_event_loop()
        while self._running:
            try:
                messages = await loop.run_in_executor(
                    None, self._consumer.consume, 1000, 1
                )
                if messages is None:
                    yield None
                    continue

                message_batch = []
                for msg in messages:
                    if msg.error():
                        if msg.error().code() != KafkaException._PARTITION_EOF:
                            logger.error(
                                f"Kafka Consumer error in batch: {msg.error()}"
                            )
                        continue
                    topic = msg.topic()
                    key = self._avro_deserializer(
                        msg.key(), SerializationContext(topic, MessageField.KEY)
                    )
                    value = self._avro_deserializer(
                        msg.value(), SerializationContext(topic, MessageField.VALUE)
                    )
                    message_batch.append({"topic": topic, "key": key, "value": value})
                yield message_batch
            except Exception as e:
                logger.error(f"Error in message stream: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def generate(self, on_update: Callable[[Dict, Dict], Awaitable[None]]):
        """
        Runs the main consumer loop. Calls the on_update callback with the
        latest DataFrames whenever the state changes.
        """
        logger.info("Generate loop started. Consuming messages...")

        async for message_batch in self._message_stream():
            if not message_batch:
                continue

            state_changed = False
            async with self.lock:
                for msg_data in message_batch:
                    if (
                        msg_data is None
                        or msg_data["key"] is None
                        or msg_data["value"] is None
                    ):
                        continue
                    key = msg_data["key"]
                    rank = key.get("rnk")
                    if rank is None:
                        continue

                    topic = msg_data["topic"]
                    value = msg_data["value"]

                    if topic == "top-teams":
                        target_df = self.teams
                    elif topic == "top-players":
                        target_df = self.players
                    else:
                        target_df = None

                    if (
                        rank not in target_df.index
                        or target_df.loc[rank].to_dict() != value
                    ):
                        target_df.loc[rank] = value
                        state_changed = True

            if state_changed:
                logger.info(
                    f"Message processing complete. {len(message_batch)} messages processed, changes detected. Triggering UI update."
                )
                async with self.lock:
                    await on_update(self.teams.copy(), self.players.copy())

    def close(self):
        """A method to cleanly shut down the consumer."""
        logger.info("Closing Kafka consumer.")
        self._running = False
        self._consumer.close()


async def console_updater(teams_df: pd.DataFrame, players_df: pd.DataFrame):
    """
    This is our example callback. In Streamlit, this function would update placeholders.
    """
    print("\033[H\033[J", end="")
    print("--- REAL-TIME LEADERBOARDS (UI Updated!) ---")

    print("\n--- Top Teams DataFrame ---")
    # Using sort_index() ensures the ranks are always displayed in order 1-10.
    print(teams_df.sort_index().to_string())

    print("\n--- Top Players DataFrame ---")
    print(players_df.sort_index().to_string())

    print("-" * 42, flush=True)


async def main():
    consumer = RealtimeLeaderboardConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        topics=TOPIC_NAMES,
        group_id=CONSUMER_GROUP,
    )
    try:
        await consumer.generate(on_update=console_updater)
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutting down.")
