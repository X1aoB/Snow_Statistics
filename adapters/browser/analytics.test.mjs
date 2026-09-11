import { test } from "node:test";
import assert from "node:assert/strict";
import { createAnalytics } from "./analytics.mjs";

test("disabled adapter does not inspect storage, location, identity or network", () => {
  const hostile = new Proxy({}, { get() { throw new Error("side effect"); } });
  const adapter = createAnalytics({ enabled: false }, hostile);
  adapter.track("page_view"); adapter.stop();
  assert.equal(adapter.jump("https://snow.xiaob.dev"), "https://snow.xiaob.dev");
});

function environment(fetcher) {
  const saved = new Map(), timers = new Map(); let id = 0;
  return { saved, timers, crypto: { randomUUID: () => "00000000-0000-4000-8000-" + String(++id).padStart(12, "0") },
    navigator: {}, location: { href: "https://xiaob.dev/?secret=private" },
    localStorage: { getItem: k => saved.get(k), setItem: (k, v) => saved.set(k, v), removeItem: k => saved.delete(k) },
    setTimeout(fn) { timers.set(++id, fn); return id; }, clearTimeout(k) { timers.delete(k); }, fetch: fetcher };
}

test("allowlist removes queries and arbitrary fields, bounded retry survives server failure", async () => {
  const requests = [];
  const env = environment(async (url, options) => { requests.push(JSON.parse(options.body)); throw Error("offline"); });
  const adapter = createAnalytics({ enabled: true, consent: true, app: "mywebsite", endpoint: "https://stats.example/analytics/v1/events" }, env);
  for (let i = 0; i < 100; i++) adapter.track("page_view", { path: "/?token=secret", chat: "private" });
  adapter.track("request_complete", { request_id: "fake" });
  for (let n = 0; n < 10 && env.timers.size; n++) {
    const [id, fn] = env.timers.entries().next().value; env.timers.delete(id); fn();
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.equal(requests.length, 4); // 20 admitted, batches of 10, at most one retry.
  const text = JSON.stringify(requests);
  assert.ok(!text.includes("private") && !text.includes("secret") && !text.includes("request_complete"));
  assert.equal(requests[0].events[0].path, "/");
  env.saved.set("business-key", "keep");
  adapter.stop();
  assert.deepEqual([...env.saved.keys()], ["business-key"]);
});

test("consent absent and privacy signal prevent identifiers", () => {
  const env = environment(() => assert.fail("network"));
  createAnalytics({ enabled: true, app: "mywebsite" }, env).track("page_view");
  env.navigator.globalPrivacyControl = true;
  createAnalytics({ enabled: true, consent: true, app: "mywebsite", endpoint: "https://stats.example/analytics/v1/events" }, env).track("page_view");
  assert.equal(env.saved.size, 0);
});

test("missing or insecure collector configuration does not create identifiers", () => {
  for (const endpoint of [undefined, "", "  ", "ftp://localhost/events", "http://stats.example/events"]) {
    const env = environment(() => assert.fail("network"));
    const adapter = createAnalytics({ enabled: true, consent: true, app: "mywebsite", endpoint }, env);
    adapter.track("page_view");
    assert.equal(adapter.active, false);
    assert.equal(env.saved.size, 0);
    assert.equal(env.timers.size, 0);
  }
});

test("repeat entry clicks renew our attribution fragment while preserving business anchors", () => {
  const env = environment(() => assert.fail("network before flush"));
  const adapter = createAnalytics({ enabled: true, consent: true, app: "mywebsite", endpoint: "https://stats.example/analytics/v1/events" }, env);
  const first = adapter.jump("https://snow.xiaob.dev/");
  const second = adapter.jump(first);
  assert.notEqual(new URL(first).hash, new URL(second).hash);
  assert.equal(adapter.jump("https://snow.xiaob.dev/#business-section"), "https://snow.xiaob.dev/#business-section");
  adapter.stop();
});
