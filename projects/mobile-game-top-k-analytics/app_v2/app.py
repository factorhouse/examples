import asyncio
import logging
from functools import partial

import streamlit as st

from generator import listen_to_updates, ui_updater

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Mobile Game Analytics Dashboard (V2)",
    page_icon="🎮",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Mobile Game Analytics Dashboard (V2)")
placeholder = st.empty()

with placeholder.container():
    st.info("🔄 Connecting to data streams...")

try:
    ui_updater_callback = partial(ui_updater, placeholder)
    asyncio.run(listen_to_updates(ui_updater_callback))
except Exception as e:
    logging.error(e)
