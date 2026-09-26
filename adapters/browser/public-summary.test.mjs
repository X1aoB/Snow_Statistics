import { test } from "node:test";
import assert from "node:assert/strict";
import { renderSummary } from "./public-summary.mjs";

function element() { return { textContent: "", children: [], append(x) { this.children.push(x); }, replaceChildren() { this.children = []; } }; }
function setup(fetch) {
  const status = element(), body = element(), popular = element();
  const root = { querySelector(q) { return q === "tbody" ? body : q.includes("popularity") ? popular : status; } };
  const env = { fetch, setTimeout, clearTimeout, document: { createElement: element } };
  return { root, env, status, body, popular };
}
test("off page never fetches and outage does not invent zeros", async () => {
  const state = setup(async () => { throw Error("offline"); });
  await renderSummary(state.root, "", state.env);
  assert.equal(state.status.textContent, "统计尚未启用。");
  await renderSummary(state.root, "/summary.json", state.env);
  assert.match(state.status.textContent, /暂不可用/);
  assert.deepEqual(state.body.children, []);
});
test("archive is explicit and public data is inserted only as text", async () => {
  const state = setup(async () => ({ ok: true, json: async () => ({ schema_version: 1, status: "archived", generated_at: "2026-01-01T00:00:00Z",
    daily: [{ date: "2026-01-01", app: "mywebsite", pv: 2, uv: 1, requests: 0, successes: 0 }],
    popularity: [{ date: "2026-01-01", app: "mywebsite", kind: "page", name: "<script>never execute</script>", count: 2 }] }) }));
  await renderSummary(state.root, "/summary.json", state.env);
  assert.match(state.status.textContent, /历史归档/);
  assert.equal(state.body.children[0].children.at(-1).textContent, "—");
  assert.match(state.popular.children[0].textContent, /<script>/);
});

function fixture() {
  return { schema_version: 2, status: "ok", generated_at: "2026-09-13T16:15:00.000Z", cutoff_at: "2026-09-13T16:15:00.000Z",
    policy: { threshold: 10, stale_after_seconds: 93600 }, daily: [{ app: "project_snow", date: "2026-09-11", cutoff_at: "2026-09-13T16:15:00.000Z",
      access: { state: "published", value: { pv: 15, uv: 10 } },
      quality: { state: "suppressed", value: null }, popularity: { state: "suppressed", value: null } }] };
}
function clocked(fetch) {
  const state = setup(fetch), intervals = new Map(), listeners = new Map();
  let milliseconds = Date.parse("2026-09-13T16:15:00.000Z"), sequence = 0, mutated = null;
  state.env.Date = { now: () => milliseconds };
  state.env.setInterval = fn => { intervals.set(++sequence, fn); return sequence; };
  state.env.clearInterval = id => intervals.delete(id);
  state.env.document.documentElement = { lang: "zh-CN" };
  state.env.document.visibilityState = "visible";
  state.env.document.addEventListener = (type, fn) => listeners.set(type, fn);
  state.env.document.removeEventListener = type => listeners.delete(type);
  state.env.MutationObserver = class { constructor(fn) { mutated = fn; } observe() {} disconnect() { mutated = null; } };
  return { ...state, intervals, listeners, advance(ms) { milliseconds += ms; },
    tick() { for (const fn of intervals.values()) fn(); }, changeLanguage(lang) { state.env.document.documentElement.lang = lang; mutated?.(); } };
}
const response = value => ({ ok: true, json: async () => value });

test("v2 shows independent sample states without fabricating zero or exposing hidden values", async () => {
  const state = clocked(async () => response(fixture()));
  const stop = await renderSummary(state.root, "/v2/summary.json", state.env);
  assert.deepEqual(state.body.children[0].children.map(x => x.textContent), ["2026-09-11", "小吉终端", "15", "10", "样本不足", "样本不足"]);
  assert.match(state.popular.children[0].textContent, /样本不足/);
  state.changeLanguage("en");
  assert.equal(state.body.children[0].children[4].textContent, "Insufficient sample");
  assert.match(state.status.textContent, /Delayed public aggregates/);
  stop();
  assert.equal(state.intervals.size, 0);
  assert.equal(state.listeners.size, 0);
});

test("pending, no data and unavailable remain distinguishable", async () => {
  const data = fixture();
  data.daily[0].access = { state: "pending", value: null };
  data.daily[0].quality = { state: "empty", value: null };
  const state = clocked(async () => response(data));
  const stop = await renderSummary(state.root, "/v2/summary.json", state.env);
  assert.equal(state.body.children[0].children[2].textContent, "等待公开");
  assert.equal(state.body.children[0].children[4].textContent, "无数据");
  stop();
  const unavailable = clocked(async () => response({ ...data, generated_at: null, cutoff_at: null, status: "unavailable", daily: [] }));
  (await renderSummary(unavailable.root, "/v2/summary.json", unavailable.env))();
  assert.match(unavailable.status.textContent, /暂不可用/);
  assert.deepEqual(unavailable.body.children, []);
});

test("a long-open hidden tab rejudges the 26-hour cutoff without fetching", async () => {
  let calls = 0;
  const state = clocked(async () => { calls += 1; return response(fixture()); });
  state.env.document.visibilityState = "hidden";
  const stop = await renderSummary(state.root, "/v2/summary.json", state.env);
  state.advance(26 * 60 * 60 * 1000);
  state.tick();
  assert.doesNotMatch(state.status.textContent, /历史结果/);
  state.advance(1);
  state.tick();
  assert.match(state.status.textContent, /历史结果/);
  assert.equal(calls, 1);
  stop();
});

test("visible refresh is bounded and an outage keeps a dated last result", async () => {
  let calls = 0;
  const state = clocked(async () => { calls += 1; if (calls > 1) throw Error("offline"); return response(fixture()); });
  const stop = await renderSummary(state.root, "/v2/summary.json", state.env);
  state.advance(4 * 60 * 1000); state.tick();
  assert.equal(calls, 1);
  state.advance(60 * 1000); state.tick(); state.tick();
  await new Promise(setImmediate);
  assert.equal(calls, 2);
  assert.match(state.status.textContent, /暂不可用，显示上次结果.*更新于/);
  assert.equal(state.body.children[0].children[2].textContent, "15");
  stop();
});

test("validation rejects non-null suppressed values and unsafe quality subtotals before painting", async () => {
  for (const value of [{ state: "suppressed", value: { requests: 1, successes: 1, success_rate: 1 } },
    { state: "published", value: { requests: 11, successes: 10, success_rate: 10 / 11 } }]) {
    const data = fixture(); data.daily[0].quality = value;
    const state = clocked(async () => response(data));
    (await renderSummary(state.root, "/v2/summary.json", state.env))();
    assert.deepEqual(state.body.children, []);
    assert.match(state.status.textContent, /暂不可用/);
  }
});

test("rendering again releases the previous refresh loop", async () => {
  const state = clocked(async () => response(fixture()));
  await renderSummary(state.root, "/v2/summary.json", state.env);
  assert.equal(state.intervals.size, 1);
  const stop = await renderSummary(state.root, "/v2/summary.json", state.env);
  assert.equal(state.intervals.size, 1);
  stop();
  assert.equal(state.intervals.size, 0);
});
