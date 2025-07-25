import asyncio
from functools import partial

import streamlit as st

from generator import listen_to_updates, ui_updater

st.set_page_config(
    page_title="Mobile Game Analytics",
    page_icon="🎮",
    layout="wide",
)

st.title("Mobile Game Analytics Dashboard")
placeholder = st.empty()

with placeholder.container():
    st.info("🔄 Connecting to data streams...")

ui_updater_callback = partial(ui_updater, placeholder)
asyncio.run(listen_to_updates(ui_updater_callback))
