import asyncio
import logging
import os
import datetime
from typing import Dict, List, Callable, Awaitable

import streamlit as st
from streamlit.delta_generator import DeltaGenerator
import altair as alt
import pandas as pd
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC_NAMES = ["top-teams", "top-players", "hot-streakers", "team-mvps"]
CONSUMER_GROUP = "mobile-analytics-top-k"


class TopicConsumer:
    """A dedicated Kafka consumer for a single topic."""

    def __init__(self, bootstrap_servers: str, schema_registry_url: str, topic: str):
        logger.info(f"Initializing dedicated consumer for topic '{topic}'")
        self.topic = topic
        self._running = True

        # Each consumer gets its own group ID to ensure it reads independently.
        client_conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self._consumer = Consumer(client_conf)
        self._consumer.subscribe([self.topic])

        registry_conf = {
            "url": schema_registry_url,
            "basic.auth.user.info": "admin:admin",
        }
        schema_registry_client = SchemaRegistryClient(registry_conf)
        self._avro_deserializer = AvroDeserializer(schema_registry_client)

    async def message_stream(self):
        """An async generator that yields deserialized messages from its topic."""
        loop = asyncio.get_event_loop()
        while self._running:
            try:
                msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaException._PARTITION_EOF:
                        logger.error(
                            f"Kafka Error on topic {self.topic}: {msg.error()}"
                        )
                    continue

                key = self._avro_deserializer(
                    msg.key(), SerializationContext(self.topic, MessageField.KEY)
                )
                value = self._avro_deserializer(
                    msg.value(), SerializationContext(self.topic, MessageField.VALUE)
                )
                yield {"topic": self.topic, "key": key, "value": value}
            except Exception as e:
                logger.error(
                    f"Error in consumer for topic {self.topic}: {e}", exc_info=True
                )
                await asyncio.sleep(5)

    def close(self):
        self._running = False
        self._consumer.close()


class AnalyticsOrchestrator:
    """
    Manages multiple TopicConsumers, aggregates their data into a unified
    state, and triggers UI updates periodically.
    """

    def __init__(
        self, bootstrap_servers: str, schema_registry_url: str, topics: List[str]
    ):
        self.teams = pd.DataFrame(
            columns=["rnk", "team_id", "team_name", "total_score"]
        ).set_index("rnk")
        self.players = pd.DataFrame(
            columns=["rnk", "user_id", "team_name", "total_score"]
        ).set_index("rnk")
        self.streakers = pd.DataFrame(
            columns=[
                "rnk",
                "user_id",
                "short_term_avg",
                "long_term_avg",
                "peak_hotness",
            ]
        ).set_index("rnk")
        self.mvps = pd.DataFrame(
            columns=[
                "rnk",
                "user_id",
                "team_name",
                "player_total",
                "team_total",
                "contrib_ratio",
            ]
        ).set_index("rnk")
        self.df_map = {
            "top-teams": self.teams,
            "top-players": self.players,
            "hot-streakers": self.streakers,
            "team-mvps": self.mvps,
        }

        self.lock = asyncio.Lock()
        self._state_changed_in_batch = False

        self.consumers = [
            TopicConsumer(bootstrap_servers, schema_registry_url, topic)
            for topic in topics
        ]

    async def _process_message(self, msg_data: Dict):
        """Safely updates the internal DataFrame state."""
        async with self.lock:
            rank = msg_data["key"].get("rnk")
            if rank is None:
                return

            topic, value = msg_data["topic"], msg_data["value"]
            target_df = self.df_map.get(topic)

            if (
                value is not None
                and target_df is not None
                and (
                    rank not in target_df.index
                    or target_df.loc[rank].to_dict() != value
                )
            ):
                target_df.loc[rank] = value
                self._state_changed_in_batch = True

    async def _run_consumer_worker(self, consumer: TopicConsumer):
        """A small worker task to process messages from one consumer."""
        async for msg in consumer.message_stream():
            await self._process_message(msg)

    async def _periodic_ui_updater(self, on_update: Callable[[...], Awaitable[None]]):  # type: ignore
        """The task that checks for state changes and calls the UI callback."""
        while True:
            await asyncio.sleep(2.0)  # Check for updates every two seconds
            if self._state_changed_in_batch:
                logger.info(
                    "Changes detected since last update. Triggering UI refresh."
                )
                async with self.lock:
                    self._state_changed_in_batch = False
                    # Pass copies to prevent race conditions
                    await on_update(
                        self.teams.copy(),
                        self.players.copy(),
                        self.streakers.copy(),
                        self.mvps.copy(),
                        last_updated=datetime.datetime.now(datetime.timezone.utc),
                    )

    async def generate(self, on_update: Callable[[Dict, Dict], Awaitable[None]]):
        """
        The main entry point. Runs all consumer workers and the UI updater
        concurrently.
        """
        logger.info("Starting orchestrator and all dedicated consumers.")

        # Create a task for each consumer to run in the background
        consumer_tasks = [self._run_consumer_worker(c) for c in self.consumers]

        # Create a task for the UI updater to run in the background
        updater_task = self._periodic_ui_updater(on_update)

        # Use asyncio.gather to run all tasks concurrently
        await asyncio.gather(*consumer_tasks, updater_task)

    def close(self):
        """Closes all consumers and deletes their associated consumer groups."""
        logger.info("Closing all topic consumers...")
        for consumer in self.consumers:
            consumer.close()


async def listen_to_updates(on_update: Callable[[Dict, Dict], Awaitable[None]]):
    """
    The public-facing function that `app.py` calls. Its signature is unchanged.
    """
    orchestrator = AnalyticsOrchestrator(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        topics=TOPIC_NAMES,
    )
    try:
        await orchestrator.generate(on_update=on_update)
    finally:
        orchestrator.close()


async def console_updater(teams_df, players_df, streakers_df, mvps_df, last_updated):
    """A callback function to print the updated leaderboards to the console."""
    print("\033[H\033[J", end="")
    print(f"--- LAST UPDATED AT {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
    print("--- REAL-TIME LEADERBOARDS (DEDICATED CONSUMERS) ---")
    print("\n--- Top Teams DataFrame ---")
    print(teams_df.sort_index().to_string())
    print("\n--- Top Players DataFrame ---")
    print(players_df.sort_index().to_string())
    print("\n--- Hot Streakers DataFrame ---")
    print(streakers_df.sort_index().to_string())
    print("\n--- Team MVPs DataFrame ---")
    print(mvps_df.sort_index().to_string())
    print("-" * 42, flush=True)


def make_score_bar_chart(df, y_col, value_col, label="Score"):
    """Creates a horizontal Altair bar chart for displaying absolute scores."""
    chart_df = df.copy()
    return (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{value_col}:Q", title=label, axis=alt.Axis(format=",.0f")),
            y=alt.Y(f"{y_col}:N", title=y_col.replace("_", " ").title(), sort="-x"),
            color=alt.Color(f"{y_col}:N", legend=None),
            tooltip=[
                alt.Tooltip(f"{y_col}:N", title=y_col),
                alt.Tooltip(f"{value_col}:Q", format=",.0f", title=label),
            ],
        )
        .properties(width=600, height=400)
        .interactive()
    )


def make_percentage_bar_chart(df, y_col, value_col, percentage=True):
    """Creates a horizontal Altair bar chart for displaying percentage-based values."""
    chart_df = df.copy()
    if percentage:
        chart_df[value_col] *= 100
    return (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{value_col}:Q", title="%", axis=alt.Axis(format=".1f")),
            y=alt.Y(f"{y_col}:N", title=y_col.replace("_", " ").title(), sort="-x"),
            color=alt.Color(f"{y_col}:N", legend=None),
            tooltip=[
                alt.Tooltip(f"{y_col}:N", title=y_col),
                alt.Tooltip(f"{value_col}:Q", format=".1f", title="Value (%)"),
            ],
        )
        .properties(width=600, height=400)
        .interactive()
    )


async def ui_updater(
    placeholder: DeltaGenerator,
    teams_df,
    players_df,
    streakers_df,
    mvps_df,
    last_updated,
):
    """An async callback function to update the Streamlit UI with the latest data."""
    with placeholder.container():
        st.caption(f"Last updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
        cols = st.columns(2)
        with cols[0]:
            st.subheader("🏆 Top Teams")
            st.caption("Top teams with the highest scores globally.")
            st.altair_chart(
                make_score_bar_chart(teams_df, "team_name", "total_score"),
                use_container_width=True,
            )
        with cols[1]:
            st.subheader("🥇 Top Players")
            st.caption("Top players with the highest scores globally.")
            st.altair_chart(
                make_score_bar_chart(players_df, "user_id", "total_score"),
                use_container_width=True,
            )
        cols2 = st.columns(2)
        with cols2[0]:
            st.subheader("🔥 Hot Streakers")
            st.caption(
                "Top players with strongest scoring momentum, based on short-term vs. long-term performance."
            )
            st.altair_chart(
                make_percentage_bar_chart(streakers_df, "user_id", "peak_hotness"),
                use_container_width=True,
            )
        with cols2[1]:
            st.subheader("⭐ Team MVPs")
            st.caption("Team MVPs who outperform their own teammates.")
            st.altair_chart(
                make_percentage_bar_chart(mvps_df, "user_id", "contrib_ratio"),
                use_container_width=True,
            )


if __name__ == "__main__":
    try:
        asyncio.run(listen_to_updates(console_updater))
    except KeyboardInterrupt:
        logger.info("Application shutting down.")
