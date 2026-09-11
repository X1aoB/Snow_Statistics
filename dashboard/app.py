"""Local synthetic laboratory UI. Never expose this app through the public proxy."""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from snow_statistics.publication import connect, read_published

st.set_page_config(page_title="Snow Statistics Lab", layout="wide")
st.title("Snow Statistics · 数据开发实验室")
st.caption("合成数据 / 本地实验结果；不是线上用户规模或生产性能证明")
mode = st.sidebar.radio("数据来源", ["Doris 已发布数仓结果", "Python 正确性基准"])
if mode == "Doris 已发布数仓结果":
    st.caption("Spark → HDFS / Hive → 校验 → Doris 发布版本；仅查询 synthetic 来源")
    try:
        with connect() as db:
            published = read_published(db, "synthetic")
    except Exception:
        st.warning("数仓暂不可用。启动分析节点并配置 SNOW_DORIS_HOST 后重试；此状态不代表指标为零。")
        st.stop()
    if not published["releases"]:
        st.info("尚无通过校验的离线发布。")
        st.stop()
    daily = pd.DataFrame(published["daily"])
    st.subheader("每日活跃与服务质量")
    if daily.empty:
        st.info("已发布日期范围内没有有效事件。")
    else:
        display = daily.assign(success_rate=[f"{100 * row.successes / row.requests:.1f}%" if row.requests else "—"
                                             for row in daily.itertuples()])
        display = display.rename(columns={"source": "来源", "app": "应用", "date": "业务日期（香港）",
                                          "pv": "PV", "uv": "每日 UV", "requests": "请求量",
                                          "successes": "成功数", "success_rate": "成功率"})
        st.dataframe(display, use_container_width=True, hide_index=True)
        st.line_chart(daily.groupby("date")[["pv", "requests", "successes"]].sum())
        st.caption("每日 UV 按应用分别展示；未相加为跨日期去重人数。无请求时成功率为空。")
    st.subheader("发布日期与数据截止时间")
    st.dataframe(pd.DataFrame(published["releases"]).rename(columns={"date": "业务日期（香港）",
                  "run_id": "发布任务", "cutoff": "数据截止时间（UTC）"}), use_container_width=True, hide_index=True)
    st.stop()
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
