/** Runtime-only public aggregates. No build fetch, browser identity or API HTML. */
const active = new WeakMap();
const STALE_MS = 26 * 60 * 60 * 1000;
const REFRESH_MS = 5 * 60 * 1000;
const dictionaries = {
  zh: { disabled: "统计尚未启用。", unavailable: "统计暂不可用，请稍后再试。", ok: "延迟公开的汇总统计",
    empty: "尚无可公开的数据", stale: "历史结果，等待更新", archived: "历史归档", updated: "更新于",
    pending: "等待公开", suppressed: "样本不足", noData: "无数据", website: "个人网站", snow: "小吉终端",
    offline: "暂不可用，显示上次结果" },
  en: { disabled: "Statistics are not enabled.", unavailable: "Statistics are temporarily unavailable.",
    ok: "Delayed public aggregates", empty: "No public data yet", stale: "Previous results, awaiting an update",
    archived: "Historical archive", updated: "Updated", pending: "Awaiting publication", suppressed: "Insufficient sample",
    noData: "No data", website: "Personal website", snow: "Xiaoji Terminal", offline: "Unavailable; showing the previous result" },
};
const appName = (app, labels) => app === "mywebsite" ? labels.website : labels.snow;
const integer = value => Number.isSafeInteger(value) && value >= 0;
const date = value => typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);

function validate(data) {
  if (![1, 2].includes(data.schema_version) || !Array.isArray(data.daily) || data.daily.length > 180 ||
      !["ok", "empty", "stale", "unavailable", "archived"].includes(data.status)) throw Error("contract");
  if (data.generated_at !== null && (typeof data.generated_at !== "string" || !Number.isFinite(Date.parse(data.generated_at)))) throw Error("timestamp");
  if (data.schema_version === 2 && (data.policy?.threshold !== 10 || data.policy?.stale_after_seconds !== 93600 ||
      (data.cutoff_at !== null && !Number.isFinite(Date.parse(data.cutoff_at))))) throw Error("policy");
  for (const row of data.daily) {
    if (!["mywebsite", "project_snow"].includes(row.app) || !date(row.date)) throw Error("row");
    if (data.schema_version === 1) {
      if (![row.pv, row.uv, row.requests, row.successes].every(integer) || row.successes > row.requests) throw Error("metric");
    } else {
      if (!Number.isFinite(Date.parse(row.cutoff_at))) throw Error("cutoff");
      for (const key of ["access", "quality", "popularity"]) {
        const group = row[key];
        if (!group || !["published", "suppressed", "pending", "empty"].includes(group.state) ||
            (group.state === "published" ? group.value == null : group.value !== null)) throw Error("group");
      }
      const a = row.access.value, q = row.quality.value, heat = row.popularity.value;
      if (a && (![a.pv, a.uv].every(integer) || a.uv < 10)) throw Error("access");
      if (q && (![q.requests, q.successes].every(integer) || q.requests < 10 || q.successes > q.requests ||
        (q.successes > 0 && q.successes < 10) || (q.requests - q.successes > 0 && q.requests - q.successes < 10) ||
        !Number.isFinite(q.success_rate) || Math.abs(q.success_rate - q.successes / q.requests) > 1e-12)) throw Error("quality");
      if (heat && (!Array.isArray(heat) || heat.length > 1000 || heat.some(item =>
        !["page", "character"].includes(item.kind) || typeof item.name !== "string" || item.name.length > 200 ||
        !integer(item.count) || item.count < 10))) throw Error("popularity");
    }
  }
  return data;
}

export async function renderSummary(root, endpoint, env = globalThis) {
  if (!root) return () => {};
  active.get(root)?.();
  const status = root.querySelector("[data-statistics-status]");
  const body = root.querySelector("tbody");
  const popular = root.querySelector("[data-statistics-popularity]");
  const language = () => (root.dataset?.statisticsLocale || env.document?.documentElement?.lang || "zh").startsWith("en") ? "en" : "zh";
  const now = () => env.Date?.now?.() ?? Date.now();
  let cached = null, failed = false, stopped = false, fetching = false, controller = null, interval = null, refreshed = 0, observer = null;
  status?.setAttribute?.("aria-live", "polite");
  const visible = () => env.document?.visibilityState !== "hidden";
  const draw = () => {
    const locale = language(), labels = dictionaries[locale];
    if (!cached) { status.textContent = endpoint ? labels.unavailable : labels.disabled; return; }
    const at = cached.generated_at ? new Date(cached.generated_at) : null;
    const stale = at && now() - at.getTime() > STALE_MS;
    const state = ["archived", "unavailable"].includes(cached.status) ? cached.status : stale ? "stale" : cached.status;
    status.textContent = `${failed ? labels.offline : labels[state] || labels.unavailable}${at ? ` · ${labels.updated} ${at.toLocaleString(locale === "en" ? "en-GB" : "zh-CN", { timeZone: "Asia/Hong_Kong" })}` : ""}`;
    body.replaceChildren(); popular?.replaceChildren();
    const cell = (row, value) => { const td = env.document.createElement("td"); td.textContent = String(value); row.append(td); };
    const hidden = group => group.state === "empty" ? labels.noData : labels[group.state];
    for (const row of cached.daily) {
      const tr = env.document.createElement("tr");
      const values = [row.date, appName(row.app, labels)];
      if (cached.schema_version === 1) {
        values.push(row.pv, row.uv, row.requests, row.requests ? `${(row.successes / row.requests * 100).toFixed(1)}%` : "—");
      } else {
        values.push(row.access.value?.pv ?? hidden(row.access), row.access.value?.uv ?? hidden(row.access),
          row.quality.value?.requests ?? hidden(row.quality),
          row.quality.value ? `${(row.quality.value.success_rate * 100).toFixed(1)}%` : hidden(row.quality));
      }
      for (const value of values) cell(tr, value);
      body.append(tr);
      if (popular && cached.schema_version === 2) {
        const entries = row.popularity.value || [{ name: hidden(row.popularity), count: null }];
        for (const entry of entries) {
          const li = env.document.createElement("li");
          li.textContent = `${row.date} · ${appName(row.app, labels)} · ${entry.name}${entry.count === null ? "" : `: ${entry.count}`}`;
          popular.append(li);
        }
      }
    }
    if (popular && cached.schema_version === 1) {
      for (const row of (cached.popularity || []).slice(-50)) {
        if (!["page", "character"].includes(row.kind) || !integer(row.count) || typeof row.name !== "string") continue;
        const li = env.document.createElement("li");
        li.textContent = `${row.date} · ${appName(row.app, labels)} · ${row.name}: ${row.count}`; popular.append(li);
      }
    }
  };
  const refresh = async () => {
    if (stopped || fetching || !endpoint) return;
    fetching = true; controller = new AbortController();
    const timeout = env.setTimeout(() => controller?.abort(), 4000);
    try {
      const response = await env.fetch(endpoint, { credentials: "omit", referrerPolicy: "no-referrer", signal: controller.signal });
      if (!response.ok) throw Error("unavailable");
      const data = validate(await response.json());
      if (!stopped) { cached = data; failed = false; }
    } catch { if (!stopped) failed = true; }
    finally {
      env.clearTimeout(timeout); fetching = false; refreshed = now();
      if (!stopped) draw();
    }
  };
  const tick = () => { if (!stopped) { draw(); if (visible() && now() - refreshed >= REFRESH_MS) void refresh(); } };
  const pagehide = event => { if (!event.persisted) stop(); };
  const stop = () => {
    stopped = true; controller?.abort();
    if (interval !== null) env.clearInterval(interval);
    env.document?.removeEventListener?.("visibilitychange", tick);
    observer?.disconnect();
    env.removeEventListener?.("pagehide", pagehide);
    env.removeEventListener?.("pageshow", tick);
  };
  active.set(root, stop);
  body.replaceChildren(); popular?.replaceChildren();
  if (!endpoint) { draw(); return stop; }
  await refresh();
  if (!stopped) {
    if (env.setInterval) interval = env.setInterval(tick, 60000);
    env.document?.addEventListener?.("visibilitychange", tick);
    env.addEventListener?.("pagehide", pagehide);
    env.addEventListener?.("pageshow", tick);
    if (env.MutationObserver && env.document?.documentElement) {
      observer = new env.MutationObserver(() => { if (!stopped) draw(); });
      observer.observe(env.document.documentElement, { attributes: true, attributeFilter: ["lang"] });
    }
  }
  return stop;
}
