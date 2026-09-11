"""Local synthetic laboratory UI. Never expose this app through the public proxy."""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from snow_statistics.model_publication import validate_model
from snow_statistics.publication import connect, read_published

st.set_page_config(page_title="Snow Statistics Lab", layout="wide")
st.title("Snow Statistics · 数据开发实验室")
st.caption("合成数据 / 本地实验结果；不是线上用户规模或生产性能证明")
mode = st.sidebar.radio("数据来源", ["Doris 已发布数仓结果", "Spark 已发布运营与行为模型", "Marquez 血缘归档", "Python 正确性基准"])
if mode == "Marquez 血缘归档":
    from snow_statistics.lineage_view import render
    render()
    st.stop()
if mode == "Spark 已发布运营与行为模型":
    path = Path(st.sidebar.text_input("私有模型发布文件", "runtime/publication/models-latest.json"))
    if not path.is_file():
        st.info("尚无模型发布文件。运行 snow_models 后，将验收通过的模型发布文件同步到本机。")
        st.stop()
    try:
        data = json.loads(path.read_bytes())
        operations = validate_model(data["operations"], "operations")
        behavior = validate_model(data["behavior"], "behavior")
        if any(operations[k] != behavior[k] for k in ("input_snapshot", "date_from", "date_to", "cutoff")):
            raise ValueError("Different model windows")
    except (ValueError, KeyError, TypeError, OSError):
        st.error("模型文件未通过校验，无法展示。请重新同步验收通过的发布文件。")
        st.stop()
    st.caption(f"合成数据历史归档 · {behavior['date_from']} 至 {behavior['date_to']} · 截止 {behavior['cutoff']} · 发布 {data['run_id']}")
    st.caption("由 Spark / YARN 计算并通过 Hive 读回验证；读取本机归档时无需运行虚拟机。")
    sessions, retention, funnel, tickets = st.tabs(["会话", "留存", "渠道转化", "运营状态"])
    with sessions:
        st.dataframe(data["behavior"]["aggregates"]["session_daily"], use_container_width=True, hide_index=True)
        st.caption("30 分钟无活动结束会话；会话整体归于开始日期，跨午夜不拆分。")
    with retention:
        rows = pd.DataFrame(data["behavior"]["aggregates"]["retention"])
        if not rows.empty:
            for lag in (1, 7):
                rows[f"D{lag} 留存率"] = [f"{100 * r[f'retained_d{lag}'] / r[f'eligible_d{lag}']:.1f}%" if r[f"eligible_d{lag}"] else "尚未成熟" for r in rows.to_dict("records")]
        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.caption("按固定输入历史中首次观察日期分群；尚未完成 D1 / D7 观察日的留存为空。匿名标识不会跨应用合并。")
    with funnel:
        st.dataframe(data["behavior"]["aggregates"]["funnel"], use_container_width=True, hide_index=True)
        st.caption("成功对话归于 30 分钟内最近一次有效入口，每次入口最多转化一次。角色选择仅为诊断指标，不是转化前提。")
    with tickets:
        st.dataframe(data["operations"]["aggregates"]["ticket_daily"], use_container_width=True, hide_index=True)
        st.dataframe(data["operations"]["aggregates"]["current_categories"], use_container_width=True, hide_index=True)
        st.caption("工单为每日末次状态；分类为本次截止时间的有效分类。历史有效区间与处理轮次保存在 Hive 明细表。")
    st.stop()
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
    st.caption("早期固定样例；未包含留存成熟期。完整口径请查看 Spark 已发布运营与行为模型。")
    st.dataframe(data["conversions"], use_container_width=True)
    st.dataframe(data["retention"], use_container_width=True)
with tab3:
    st.dataframe(data["content_scd2"], use_container_width=True)
with tab4:
    st.dataframe(data["ticket_rounds"], use_container_width=True)
    st.dataframe(data["ticket_daily"], use_container_width=True)
