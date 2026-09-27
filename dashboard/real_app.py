"""Dedicated loopback-only entry point for the retained real aggregate view."""
import streamlit as st

from snow_statistics.real_view import render

st.set_page_config(page_title="Snow Statistics · 私有真实汇总", layout="wide")
render()
