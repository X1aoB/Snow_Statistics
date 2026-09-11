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
