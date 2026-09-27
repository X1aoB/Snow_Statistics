export class Element extends EventTarget {
  constructor(tag = "div") { super(); this.tagName = tag.toUpperCase(); this.children = []; this.attributes = {}; this.hidden = false; this.textContent = ""; }
  append(...nodes) { this.children.push(...nodes); }
  setAttribute(key, value) { this.attributes[key] = String(value); }
  getAttribute(key) { return this.attributes[key] ?? null; }
  focus() { this.focused = true; }
  click() { if (!this.disabled) this.dispatchEvent(new Event("click")); }
  get all() { return [this, ...this.children.flatMap(child => child.all)]; }
}

export function environment(fetcher = async () => ({ ok: true }), saved = new Map(), href = "https://xiaob.dev/?secret=private") {
  let id = 0, at = Date.parse("2026-09-13T00:00:00Z");
  const env = new EventTarget(), timers = new Map(), body = new Element("body"), footer = new Element("footer");
  body.append(footer);
  const document = new Element("document");
  Object.assign(document, { body, documentElement: { lang: "zh-CN" }, createElement: tag => new Element(tag),
    querySelector: selector => selector === "footer" ? footer : null,
    getElementById: key => body.all.find(node => node.id === key) });
  Object.assign(env, { saved, timers, document, Date: { now: () => at }, advance: ms => { at += ms; },
    crypto: { randomUUID: () => "00000000-0000-4000-8000-" + String(++id).padStart(12, "0") },
    navigator: {}, location: new URL(href),
    localStorage: { getItem: key => saved.get(key), setItem: (key, value) => saved.set(key, value), removeItem: key => saved.delete(key) },
    setTimeout(fn, delay) { timers.set(++id, { fn, delay }); return id; }, clearTimeout(key) { timers.delete(key); }, fetch: fetcher,
    history: { state: { business: "keep" }, replaceState(state, ignored, url) { env.location = new URL(url, env.location); this.state = state; } },
    MutationObserver: class { constructor(fn) { env.mutation = fn; } observe() {} },
  });
  env.emit = (type, fields = {}) => { const event = new Event(type); Object.assign(event, fields); env.dispatchEvent(event); };
  env.tick = async (delay = 1000) => {
    const timer = [...timers].find(([, value]) => value.delay === delay);
    if (!timer) return false;
    timers.delete(timer[0]); timer[1].fn(); await new Promise(resolve => setImmediate(resolve)); return true;
  };
  return env;
}

export function grant(env, app = "mywebsite", expires = env.Date.now() + 86400000) {
  env.localStorage.setItem(`snow.statistics.v1.${app}.consent`, String(expires));
}
export const config = (app = "mywebsite") => ({ enabled: true, consent: true, app, endpoint: "https://stats.example/analytics/v1/events", characters: ["sample"] });
