"""Private aggregate view gated by current local and remote real-data lifecycle."""
import os
from datetime import UTC, datetime

from .io import digest
from .publication import canonical
from .real_aggregate_read import cached_readable, read_managed_aggregate
from .real_lab import ROOT, private_relative, read_json, secret_file, validate_config


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
    config_file = st.sidebar.text_input("已登记运行配置", os.environ.get("SNOW_REAL_VIEW_CONFIG", "runtime/real/config/prod.json"))
    run_id = st.sidebar.text_input("已发布批次", os.environ.get("SNOW_REAL_VIEW_RUN_ID", ""))
    st.caption("真实来源 · 私有精确聚合 · 本机关闭不影响线上基础统计")

    @st.fragment(run_every=1)
    def body():
        cache_key = "snow_real_aggregate_admission"
        try:
            private_relative(config_file, "config", ".json")
            config = validate_config(read_json(secret_file(ROOT, config_file)))
            if (ROOT / "runtime/real/lifecycle" / config["lane"] / "cleanup.json").exists():
                raise ValueError("Remote cleanup is incomplete; discard any cached read lease")
            identity = digest(canonical([config, run_id]))
            cached = st.session_state.get(cache_key)
            if cached and cached["identity"] == identity:
                try:
                    release = cached_readable(cached["release"], cached["receipt"])
                except ValueError:
                    st.session_state.pop(cache_key, None)
                    cached = None
            else:
                st.session_state.pop(cache_key, None)
                cached = None
            if cached is None:
                release, receipt = read_managed_aggregate(config, run_id, root=ROOT)
                cached_readable(release, receipt)
                cached = dict(identity=identity, release=release, receipt=receipt)
                st.session_state[cache_key] = cached
                st.session_state[cache_key + "_cutoff"] = receipt["cutoff"]
            release = cached["release"]
            # The in-process receipt has a <=60s lease, checked each render and
            # bounded by every original expiry. No file can supply this cache.
            cached_readable(release, cached["receipt"], now=datetime.now(UTC))
        except Exception:
            st.session_state.pop(cache_key, None)
            st.warning("私有看板已暂停：资源窗口未开放，或真实数据清理/完整性校验尚未通过。不会自动启动服务，也不会显示零值代替缺失。")
            cutoff = st.session_state.get(cache_key + "_cutoff")
            if cutoff:
                st.caption("上次校验的数据截止：" + cutoff)
            return
        if cached["receipt"]["input_origin"] != "real":
            st.warning("当前输入为合成验收样例，不是正式业务统计。")
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
