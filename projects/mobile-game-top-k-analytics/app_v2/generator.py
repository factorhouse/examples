import asyncio
import logging
import os
import datetime
from typing import Callable, Awaitable

import streamlit as st
from streamlit.delta_generator import DeltaGenerator
import altair as alt
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fh_dev")
DB_USER = os.getenv("DB_USER", "db_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "db_password")
DB_SCHEMA = os.getenv("DB_SCHEMA", "demo")
POLL_INTERVAL_SECONDS = 2
TABLE_NAMES = ["top_teams", "top_players", "hot_streaks", "team_mvps"]


async def listen_to_updates(on_update: Callable[..., Awaitable[None]]):
    """
    Polls the PostgreSQL database periodically and triggers the UI update callback.
    """
    db_uri = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(db_uri)

    logger.info(f"Starting database polling every {POLL_INTERVAL_SECONDS} seconds...")

    while True:
        try:
            with engine.connect() as conn:
                dfs = {
                    table: pd.read_sql_query(
                        f"SELECT * FROM {DB_SCHEMA}.{table} ORDER BY rnk;", conn
                    )
                    for table in TABLE_NAMES
                }

            for df in dfs.values():
                if "rnk" in df.columns:
                    df.set_index("rnk", inplace=True)

            await on_update(
                teams_df=dfs["top_teams"],
                players_df=dfs["top_players"],
                streaks_df=dfs["hot_streaks"],
                mvps_df=dfs["team_mvps"],
                last_updated=datetime.datetime.now(datetime.timezone.utc),
            )

        except Exception as e:
            logger.error(
                f"An error occurred: {e}. Retrying in {POLL_INTERVAL_SECONDS} seconds.",
                exc_info=True,
            )

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def console_updater(teams_df, players_df, streaks_df, mvps_df, last_updated):
    """A callback function to print the updated leaderboards to the console."""
    print("\033[H\033[J", end="")  # Clear console
    print(f"--- LAST UPDATED AT {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
    print("--- REAL-TIME LEADERBOARDS (FROM DATABASE) ---")
    print("\n--- Top Teams DataFrame ---")
    print(teams_df.to_string())
    print("\n--- Top Players DataFrame ---")
    print(players_df.to_string())
    print("\n--- Hot Streaks DataFrame ---")
    print(streaks_df.to_string())
    print("\n--- Team MVPs DataFrame ---")
    print(mvps_df.to_string())
    print("-" * 42, flush=True)


def make_score_bar_chart(df, y_col, value_col, label="Score"):
    """Creates a horizontal Altair bar chart for displaying absolute scores."""
    if df.empty:
        return alt.Chart(pd.DataFrame({"A": []})).mark_text(text="No data available")
    return (
        alt.Chart(df)
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


def make_percentage_bar_chart(df, y_col, value_col, title="Value (%)"):
    """Creates a horizontal Altair bar chart for displaying percentage-based values."""
    if df.empty:
        return alt.Chart(pd.DataFrame({"A": []})).mark_text(text="No data available")
    chart_df = df.copy()
    chart_df[value_col] *= 100
    return (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{value_col}:Q", title=title, axis=alt.Axis(format=".1f")),
            y=alt.Y(f"{y_col}:N", title=y_col.replace("_", " ").title(), sort="-x"),
            color=alt.Color(f"{y_col}:N", legend=None),
            tooltip=[
                alt.Tooltip(f"{y_col}:N", title=y_col),
                alt.Tooltip(f"{value_col}:Q", format=".1f", title=title),
            ],
        )
        .properties(width=600, height=400)
        .interactive()
    )


async def ui_updater(
    placeholder: DeltaGenerator,
    teams_df,
    players_df,
    streaks_df,
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
            st.subheader("🔥 Hot Streaks")
            st.caption(
                "Top players with strongest scoring momentum, based on short-term vs. long-term performance."
            )
            st.altair_chart(
                make_percentage_bar_chart(
                    streaks_df, "user_id", "peak_hotness", title="Momentum Score (%)"
                ),
                use_container_width=True,
            )
        with cols2[1]:
            st.subheader("⭐ Team MVPs")
            st.caption("Team MVPs who outperform their own teammates.")
            st.altair_chart(
                make_percentage_bar_chart(
                    mvps_df, "user_id", "contrib_ratio", title="Contribution Ratio (%)"
                ),
                use_container_width=True,
            )


if __name__ == "__main__":
    try:
        asyncio.run(listen_to_updates(console_updater))
    except KeyboardInterrupt:
        logger.info("Application shutting down.")
