"""Local synthetic laboratory UI. Never expose this app through the public proxy."""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Snow Statistics Lab", layout="wide")
st.title("Snow Statistics · 数据开发实验室")
st.caption("合成数据 / 本地实验结果；不是线上用户规模或生产性能证明")
path = Path(st.sidebar.text_input("实验结果文件", "runtime/demo/model.json"))
if not path.is_file():
    st.info("先运行 uv run snow-stats demo")
    st.stop()
data = json.loads(path.read_text(encoding="utf-8"))
cols = st.columns(4)
for col, name in zip(cols, ["raw", "valid", "duplicates", "quarantined"], strict=True):
    col.metric(name, data["quality"][name])
tab1, tab2, tab3, tab4 = st.tabs(["活跃与服务", "渠道与留存", "维度历史", "工单处理轮次"])
with tab1:
    daily = pd.DataFrame(data["daily"])
    st.dataframe(daily, use_container_width=True)
    if not daily.empty:
        st.line_chart(daily.groupby("date")[["pv", "requests", "successes"]].sum())
        st.caption("UV 按应用、日期独立展示，未累加为跨日期去重人数。")
with tab2:
    st.dataframe(data["conversions"], use_container_width=True)
    st.dataframe(data["retention"], use_container_width=True)
with tab3:
    st.dataframe(data["content_scd2"], use_container_width=True)
with tab4:
    st.dataframe(data["ticket_rounds"], use_container_width=True)
    st.dataframe(data["ticket_daily"], use_container_width=True)
