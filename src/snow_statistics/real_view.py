"""Private aggregate view; no identifiers, public exposure, or warehouse dependency."""
from pathlib import Path

from .real_publication import read_real_release


def retention_display(rows):
    result = []
    labels = {"pending": "观察期未成熟", "incomplete": "历史覆盖不足"}
    for original in rows:
        row = dict(original)
        for lag in (1, 7):
            state, count = row[f"observation_d{lag}"], row[f"eligible_d{lag}"]
            row[f"D{lag} 留存率"] = (f"{100 * row[f'retained_d{lag}'] / count:.1f}%"
                                   if state == "complete_accepted_prefix" and count else labels.get(state, "无可计算样本"))
        result.append(row)
    return result


def render():
    import streamlit as st
    directory = Path(st.sidebar.text_input("已登记真实汇总目录", "runtime/real/publication"))
    st.caption("真实来源 · 私有精确聚合 · 本机关闭不影响线上基础统计")

    @st.fragment(run_every=60)
    def body():
        try:
            release = read_real_release(directory)
        except FileNotFoundError:
            st.info("尚无可读取汇总，或历史文件已到期。请完成真实链路校验和发布后重试。")
            return
        except (ValueError, OSError, KeyError, TypeError):
            st.error("真实汇总的生命周期或完整性检查未通过，读取已停止。此状态不表示数据为零。")
            return
        manifest = release["behavior"]["manifest"]
        st.caption(f"数据日期 {manifest['date_from']} 至 {manifest['date_to']} · 本地数据截止 {manifest['cutoff']}")
        st.caption(f"读取发布 {release['run_id']} · 汇总到期 {release['expires_at']} · 仅统计已接收/记录的事件")
        daily, sessions, retained, funnel = st.tabs(["每日指标", "会话", "留存", "入口归因"])
        with daily:
            st.dataframe(release["daily"]["daily"], use_container_width=True, hide_index=True)
            st.caption("UV 为各应用每日匿名标识去重数，不代表自然人数，不相加为跨日期 UV；服务质量分母为已记录的生成请求。")
        with sessions:
            st.dataframe(release["behavior"]["aggregates"]["session_daily"], use_container_width=True, hide_index=True)
            st.caption("30 分钟无活动结束会话；跨午夜的会话按开始日期归属。")
        with retained:
            st.dataframe(retention_display(release["behavior"]["aggregates"]["retention"]), use_container_width=True, hide_index=True)
            st.caption("按最多 30 天留存窗口中的首次观察分群，不能称为生命周期首次访问或新增自然用户。历史覆盖不足与观察期未成熟分别展示。")
        with funnel:
            st.dataframe(release["behavior"]["aggregates"]["funnel"], use_container_width=True, hide_index=True)
            st.caption("成功对话归于 30 分钟内最近一次有效入口；每次入口至多一次转化。真实链路没有模拟工单和活动数据。")
    body()
