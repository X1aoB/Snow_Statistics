"""Optional Streamlit view of a bounded Marquez API receipt; no backend request."""
import json
import re
from pathlib import Path

import streamlit as st


def render():
    path = Path(st.sidebar.text_input("血缘验收归档", "runtime/lineage/backend-final.json"))
    if not path.is_file():
        st.info("先将 verify_lineage_backend.py 生成的 API 验收归档同步到本机。")
        return
    try:
        receipt = json.loads(path.read_bytes())
        if receipt["engine"] != "Marquez 0.50.0" or receipt["namespace"] != "snow-statistics.synthetic":
            raise ValueError("Unexpected source")
        nodes, edges, runs = receipt["graph"]["nodes"], receipt["graph"]["edges"], receipt["runs"]
        if len(nodes) > 200 or len(edges) > 400 or len(runs) > 1000:
            raise ValueError("Unbounded graph")
        ids = {node["id"]: "n" + str(i) for i, node in enumerate(nodes)}
        if len(ids) != len(nodes) or any(e["origin"] not in ids or e["destination"] not in ids for e in edges):
            raise ValueError("Invalid graph edges")
        dot = ['digraph { rankdir=LR; node [fontname="sans-serif", fontsize=12];']
        for node in nodes:
            identity = node["id"]
            label = identity.rsplit(":", 1)[-1]
            if node["type"] == "JOB":
                label = {"snow_models.compute_operations": "运营计算", "snow_models.compute_behavior": "行为计算", "snow_models.publish": "模型发布"}.get(label, label)
                shape = "box"
            else:
                shape = "note"
                if "hive://" in identity:
                    label = re.sub(r"snow_synthetic\.(ops|behavior)_af_[a-f0-9]{24}_t\d+_", "Hive · ", label)
                elif label.endswith("/_snapshot.json"):
                    label = "ODS 固定输入清单"
                elif label.endswith(".operations.json"):
                    label = "运营验收包"
                elif label.endswith(".behavior.json"):
                    label = "行为验收包"
                elif "/model-releases/" in label:
                    label = "不可变发布历史"
                else:
                    label = Path(label).name
            dot.append(f'{ids[identity]} [label={json.dumps(label, ensure_ascii=False)}, tooltip={json.dumps(identity)}, shape={shape}];')
        dot += [ids[e["origin"]] + " -> " + ids[e["destination"]] + ";" for e in edges]
        dot.append("}")
    except (KeyError, TypeError, ValueError, OSError):
        st.error("血缘归档无效，无法展示。")
        return
    st.caption("合成实验 · Marquez API 验收归档；图展示最近依赖，表格保留失败尝试。此处不表示服务当前在线。")
    st.graphviz_chart("\n".join(dot), use_container_width=True)
    st.dataframe(runs, use_container_width=True, hide_index=True)
    st.caption(f"{len(nodes)} 个节点 · {len(edges)} 条依赖边 · {len(runs)} 次任务运行；仅显式表与文件级血缘，未声明列级血缘。")
    with st.expander("完整数据集身份"):
        st.dataframe(nodes, use_container_width=True, hide_index=True)
