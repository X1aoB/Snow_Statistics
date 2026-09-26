import { test } from "node:test";
import assert from "node:assert/strict";
import { createAnalytics, mountAnalytics } from "./analytics.mjs";
import { config, environment, grant } from "./test-environment.mjs";

test("disabled adapter never inspects browser capabilities", () => {
  const hostile = new Proxy({}, { get() { throw Error("side effect"); } });
  for (const factory of [createAnalytics, mountAnalytics]) {
    const adapter = factory({ enabled: false }, hostile);
    adapter.track("page_view"); adapter.stop();
    assert.equal(adapter.jump("https://snow.xiaob.dev"), "https://snow.xiaob.dev");
  }
});

test("allowlist excludes secrets and bounded retry never exceeds admitted events", async () => {
  const requests = [], env = environment(async (_, options) => { requests.push(JSON.parse(options.body)); throw Error("offline"); });
  grant(env); const adapter = createAnalytics(config(), env);
  for (let i = 0; i < 100; i++) adapter.track("page_view", { path: "/?token=secret", chat: "private" });
  adapter.track("request_complete", { request_id: "fake" });
  for (let n = 0; n < 10 && await env.tick(); n++) {}
  assert.equal(requests.length, 4);
  const text = JSON.stringify(requests);
  assert.ok(!text.includes("private") && !text.includes("secret") && !text.includes("request_complete"));
  assert.equal(requests[0].events[0].path, "/");
  env.saved.set("business-key", "keep"); adapter.stop();
  assert.deepEqual([...env.saved.keys()], ["business-key"]);
});

test("consent absence, privacy signals and insecure configuration never identify", () => {
  for (const setup of [() => {}, env => { grant(env); env.navigator.globalPrivacyControl = true; },
    env => { grant(env); env.navigator.doNotTrack = "1"; }]) {
    const env = environment(() => assert.fail("network")); setup(env);
    const before = new Map(env.saved);
    assert.equal(createAnalytics(config(), env).active, false);
    assert.deepEqual(env.saved, before);
  }
  for (const endpoint of [undefined, "", "  ", "ftp://localhost/events", "http://stats.example/events"]) {
    const env = environment(); grant(env);
    assert.equal(createAnalytics({ ...config(), endpoint }, env).active, false);
    assert.equal(env.saved.size, 1); assert.equal(env.timers.size, 0);
  }
});

test("expiry is checked before tracking and before queued flush", async () => {
  for (const operation of ["track", "flush"]) {
    const env = environment(() => assert.fail("expired consent sent data"));
    grant(env, "mywebsite", env.Date.now() + 500);
    const adapter = createAnalytics(config(), env); adapter.track("page_view");
    env.advance(501);
    if (operation === "track") adapter.track("page_view"); else await env.tick();
    assert.equal(adapter.active, false); assert.equal(env.timers.size, 0); assert.equal(env.saved.size, 0);
  }
});

test("expiry timer stops an idle mounted adapter and removes the optional request hook", async () => {
  const env = environment(); grant(env, "project_snow", env.Date.now() + 500);
  const adapter = mountAnalytics(config("project_snow"), env);
  assert.equal(typeof env.snowStatisticsRequest, "function"); env.advance(500); await env.tick(500);
  assert.equal(adapter.active, false); assert.equal(env.snowStatisticsRequest, undefined);
});

test("cross-tab withdrawal aborts in-flight work and stops other tabs without touching business storage", async () => {
  const shared = new Map(), signals = [];
  const pending = (_, options) => new Promise((resolve, reject) => {
    signals.push(options.signal); options.signal.addEventListener("abort", () => reject(Error("aborted")));
  });
  const first = environment(pending, shared), second = environment(pending, shared);
  grant(first); shared.set("business-key", "keep"); shared.set("snow.statistics.v1.project_snow.anonymous", "other-origin");
  const a = mountAnalytics(config(), first), b = mountAnalytics(config(), second);
  await first.tick(); await second.tick(); a.stop();
  second.emit("storage", { key: "snow.statistics.v1.mywebsite.consent" });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(b.active, false); assert.ok(signals.every(signal => signal.aborted));
  assert.equal(first.timers.size, 0); assert.equal(second.timers.size, 0);
  assert.deepEqual([...shared.keys()], ["business-key", "snow.statistics.v1.project_snow.anonymous"]);
});

test("new privacy signal and denied storage stop collection during an existing session", () => {
  for (const revoke of [env => { env.navigator.globalPrivacyControl = true; },
    env => { env.localStorage.setItem = () => { throw Error("denied"); }; }]) {
    const env = environment(); grant(env); const adapter = createAnalytics(config(), env);
    adapter.track("page_view"); revoke(env); adapter.track("page_view");
    assert.equal(adapter.active, false); assert.equal(env.saved.size, 0); assert.equal(env.timers.size, 0);
  }
});

test("repeat clicks renew attribution; withdrawal removes our fragment and preserves business anchors", () => {
  const env = environment(); grant(env); const adapter = mountAnalytics(config(), env);
  const link = { href: "https://snow.xiaob.dev/", closest: () => link };
  const click = () => { const event = new Event("click"); Object.defineProperty(event, "target", { value: { closest: selector => selector === "a[href]" ? link : null } }); env.document.dispatchEvent(event); };
  click(); const first = link.href; click(); assert.notEqual(link.href, first);
  assert.equal(adapter.jump("https://snow.xiaob.dev/#business-section"), "https://snow.xiaob.dev/#business-section");
  adapter.stop(); assert.equal(link.href, "https://snow.xiaob.dev/");
  assert.equal(adapter.jump(first), "https://snow.xiaob.dev/");
});

test("destination discards attribution without consent and preserves business history state", () => {
  for (const hash of ["#snow_jump=00000000-0000-4000-8000-000000000001", "#business-section"]) {
    const env = environment(undefined, new Map(), "https://snow.xiaob.dev/?business=keep" + hash);
    assert.equal(mountAnalytics(config("project_snow"), env).active, false);
    assert.equal(env.location.hash, hash.startsWith("#snow_jump=") ? "" : hash);
    assert.equal(env.location.search, "?business=keep"); assert.deepEqual(env.history.state, { business: "keep" });
    assert.equal(env.saved.size, 0);
  }
});
