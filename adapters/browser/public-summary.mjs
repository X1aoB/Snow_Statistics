/** Runtime-only rendering. No build-time fetch and no HTML from the API. */
export async function renderSummary(root, endpoint, env = globalThis) {
  if (!root) return;
  const status = root.querySelector("[data-statistics-status]");
  const body = root.querySelector("tbody");
  if (!endpoint) { status.textContent = "统计尚未启用。"; return; }
  const controller = new AbortController();
  const timeout = env.setTimeout(() => controller.abort(), 4000);
  try {
    const response = await env.fetch(endpoint, { credentials: "omit", referrerPolicy: "no-referrer", signal: controller.signal });
    if (!response.ok) throw new Error("unavailable");
    const data = await response.json();
    if (data.schema_version !== 1 || !Array.isArray(data.daily) || data.daily.length > 180) throw Error("contract");
    const at = data.generated_at ? new Date(data.generated_at) : null;
    if (at && Number.isNaN(at.getTime())) throw Error("timestamp");
    const stale = at && Date.now() - at.getTime() > 180000;
    const labels = { ok: "已接收事件的统计", empty: "尚无已接收的数据", stale: "历史结果，等待更新", archived: "历史归档", unavailable: "暂不可用" };
    status.textContent = `${labels[stale && data.status === "ok" ? "stale" : data.status] || "暂不可用"}${at ? ` · 更新于 ${at.toLocaleString("zh-CN", { timeZone: "Asia/Hong_Kong" })}` : ""}`;
    body.replaceChildren();
    for (const row of data.daily) {
      if (!["mywebsite", "project_snow"].includes(row.app) || !/^\d{4}-\d{2}-\d{2}$/.test(row.date)) throw Error("row");
      if (![row.pv, row.uv, row.requests, row.successes].every(n => Number.isSafeInteger(n) && n >= 0) || row.successes > row.requests) throw Error("metric");
      const tr = env.document.createElement("tr");
      for (const value of [row.date, row.app === "mywebsite" ? "个人网站" : "小吉终端", row.pv, row.uv, row.requests,
        row.requests ? `${(row.successes / row.requests * 100).toFixed(1)}%` : "—"]) {
        const td = env.document.createElement("td"); td.textContent = String(value); tr.append(td);
      }
      body.append(tr);
    }
    const popular = root.querySelector("[data-statistics-popularity]");
    if (popular) {
      popular.replaceChildren();
      for (const row of (data.popularity || []).slice(-50)) {
        if (!["page", "character"].includes(row.kind) || !Number.isSafeInteger(row.count)) continue;
        const li = env.document.createElement("li");
        li.textContent = `${row.date} · ${row.app} · ${row.name}：${row.count}`; popular.append(li);
      }
    }
  } catch {
    body.replaceChildren();
    status.textContent = "统计暂不可用，请稍后再试。";
  } finally { env.clearTimeout(timeout); }
}
