import { test } from "node:test";
import assert from "node:assert/strict";
import { installAnalytics } from "./bootstrap.mjs";
import { config, environment, grant } from "./test-environment.mjs";

function ui(env) {
  const panel = env.document.getElementById("snow-statistics-notice");
  const buttons = panel.all.filter(node => node.tagName === "BUTTON");
  return { panel, allow: buttons[0], decline: buttons[1], close: buttons[2], settings: env.document.getElementById("snow-statistics-settings") };
}

test("disabled notice does not inspect environment", () => {
  installAnalytics({ enabled: false }, new Proxy({}, { get() { assert.fail("side effect"); } }));
});

test("visible bilingual notice defaults off and exposes independent quality processing", () => {
  const env = environment(); env.document.documentElement.lang = "en";
  installAnalytics(config(), env); const state = ui(env);
  assert.equal(state.panel.hidden, false); assert.equal(state.settings.getAttribute("aria-expanded"), "true");
  assert.match(state.panel.all.map(node => node.textContent).join(" "), /Separate service quality statistics/);
  assert.match(state.allow.textContent, /Allow visit/); assert.equal(env.saved.size, 0);
  env.document.documentElement.lang = "zh-CN"; env.mutation(); assert.equal(state.allow.textContent, "允许访问统计");
  state.close.click(); assert.equal(state.panel.hidden, true); assert.equal(state.settings.focused, true);
  state.settings.click(); assert.equal(state.panel.hidden, false);
});

test("decline persists a preference without creating identifiers, and later grant never replays an old arrival", async () => {
  const events = [], env = environment(async (_, options) => { events.push(...JSON.parse(options.body).events); return { ok: true }; }, new Map(),
    "https://snow.xiaob.dev/#snow_jump=00000000-0000-4000-8000-000000000001");
  installAnalytics(config("project_snow"), env); const state = ui(env);
  assert.equal(env.location.hash, ""); state.decline.click();
  assert.equal(env.saved.size, 1); assert.ok(Number([...env.saved.values()][0]) < 0);
  state.settings.click(); state.allow.click(); await env.tick();
  assert.deepEqual(events.map(event => event.event_type), ["page_view"]);
  assert.equal(state.panel.hidden, true);
  state.settings.click(); state.decline.click();
  assert.equal(env.snowStatisticsRequest, undefined); assert.equal(env.saved.size, 1);
  assert.match(state.settings.textContent, /已关闭/);
});

test("privacy signal prevents grant; unavailable preference storage remains off", () => {
  for (const prepare of [env => { env.navigator.globalPrivacyControl = true; }, env => { env.localStorage.setItem = () => { throw Error("denied"); }; }]) {
    const env = environment(() => assert.fail("network")); prepare(env);
    installAnalytics(config(), env); const state = ui(env); state.allow.click();
    assert.equal(env.saved.size, 0); assert.equal(env.timers.size, 0); assert.match(state.settings.textContent, /已关闭/);
  }
});

test("persisted consent expires on an open page and updates the settings state", async () => {
  const env = environment(); grant(env, "mywebsite", env.Date.now() + 500);
  installAnalytics(config(), env); const state = ui(env);
  assert.match(state.settings.textContent, /已开启/); env.advance(500); await env.tick(500);
  assert.match(state.settings.textContent, /已关闭/); assert.equal(env.saved.size, 0);
});

test("a revoked preference from another tab stops the local notice adapter", () => {
  const env = environment(); grant(env); installAnalytics(config(), env); const state = ui(env);
  env.saved.set("snow.statistics.v1.mywebsite.consent", String(-(env.Date.now() + 1000)));
  env.emit("storage", { key: "snow.statistics.v1.mywebsite.consent" });
  assert.match(state.settings.textContent, /已关闭/); assert.equal(env.saved.size, 1); assert.equal(env.timers.size, 0);
});
