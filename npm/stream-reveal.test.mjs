// Stream-reveal pipeline tests — pacing cursor, parse throttling, cached
// code highlighting and the reveal span planner.
//
// Cases are ported from desktop-cc-gui (MIT):
//   tests/stream-reveal.test.ts, tests/throttled-text.test.ts,
//   tests/cached-highlight.test.ts (adapted to the string-based cache),
// plus stub-tree coverage for the DOM walker this port adds.
// npm/fixtures/omp-arrival-cadence.json is the same upstream trace fixture.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

// The pipeline is a classic script exposing globalThis.DamonStreamReveal —
// load it the same way a browser would.
(0, eval)(readFileSync(join(ROOT, "src", "stream-reveal.js"), "utf8"));
const R = globalThis.DamonStreamReveal;
const { StreamReveal, ThrottledText } = R;

function revealClock() {
  let now = 0, id = 0;
  const frames = new Map();
  const timers = new Map();
  const api = {
    now: () => now,
    frame: (cb) => { frames.set(++id, cb); return id; },
    cancelFrame: (id) => { frames.delete(id); },
    timeout: (callback, ms) => { timers.set(++id, { time: now + ms, callback }); return id; },
    clearTimeout: (id) => { timers.delete(id); },
  };
  return {
    api,
    pending: () => frames.size + timers.size,
    advance(ms, raf = true) {
      now += ms;
      if (raf) { const callbacks = [...frames.values()]; frames.clear(); callbacks.forEach((cb) => cb()); }
      for (const [id, timer] of [...timers]) if (timer.time <= now) { timers.delete(id); timer.callback(); }
    },
  };
}

function throttleClock() {
  let now = 0, nextId = 0;
  const timers = new Map();
  const api = {
    now: () => now,
    timeout: (run, ms) => { timers.set(++nextId, { at: now + ms, run }); return nextId; },
    clearTimeout: (id) => { timers.delete(id); },
  };
  return {
    api,
    pending: () => timers.size,
    advance(ms) {
      now += ms;
      for (const [id, timer] of [...timers]) if (timer.at <= now) { timers.delete(id); timer.run(); }
    },
  };
}

// ---------------------------------------------------------------- reveal —

test("one burst becomes multiple monotonic frames and catches up within 240ms", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  reveal.update("x".repeat(100), true);
  const sizes = [];
  for (let i = 0; i < 15; i++) { c.advance(16); sizes.push(reveal.read(0, 100)); }
  assert.ok(sizes[0] > 0 && sizes[0] < 20);
  assert.ok(sizes.every((value, i) => i === 0 || value >= sizes[i - 1]));
  assert.equal(sizes.at(-1), 100);
  assert.equal(c.pending(), 0);
});

test("continuous arrivals do not reset visible text or delay earlier content", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  reveal.update("x".repeat(100), true); c.advance(32);
  const before = reveal.read(0, 100);
  assert.ok(before > 0 && before < 100);
  reveal.update("x".repeat(200), true); c.advance(32);
  assert.ok(reveal.read(0, 200) > before);
  for (let i = 0; i < 13; i++) c.advance(16);
  assert.equal(reveal.read(0, 200), 200);
});

test("finish, hidden/reduced-motion, replacement and truncation reveal exact content immediately", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  reveal.update("original text", true); c.advance(16);
  reveal.update("replacement", true); assert.equal(reveal.read(0, 100), 11);
  reveal.update("rep", true); assert.equal(reveal.read(0, 100), 3);
  reveal.update("replacement", false); assert.equal(reveal.read(0, 100), 11);
  reveal.update("replacement append", true); reveal.finish();
  assert.equal(reveal.read(0, 100), 18); assert.equal(c.pending(), 0);
});

test("history shows instantly; suspended frames catch up via fallback; disposal cancels work", () => {
  const c = revealClock(), history = new StreamReveal(false, c.api);
  assert.equal(history.read(0, 10), 10);
  history.update("history", false); assert.equal(history.read(0, 100), 7);
  const live = new StreamReveal(true, c.api);
  live.update("stream", true); c.advance(100, false); assert.equal(live.read(0, 100), 6);
  live.update("stream more", true); live.cancel(); assert.equal(c.pending(), 0);
});

test("completed text runs are not notified on subsequent reveal frames", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api); let first = 0, last = 0;
  reveal.subscribe(0, 10, () => first++); const off = reveal.subscribe(10, 90, () => last++);
  reveal.update("x".repeat(100), true);
  while (reveal.read(0, 100) < 10) c.advance(16);
  const settledCalls = first;
  for (let i = 0; i < 15; i++) c.advance(16);
  assert.equal(first, settledCalls); assert.ok(last > first); off();
});

test("no intermediate prefix splits CJK, emoji, flags or combining characters", () => {
  for (const text of ["你好世界", "A🙂B", "👩‍💻完成", "🇨🇳🇸🇬", "e\u0301clair"]) {
    const boundaries = new Set([0, ...[...new Intl.Segmenter(undefined, { granularity: "grapheme" }).segment(text)].map((s) => s.index + s.segment.length)]);
    for (let n = 0; n <= text.length; n++) {
      const part = R.visiblePrefix(text, n);
      assert.ok(boundaries.has(part.length)); assert.ok(text.startsWith(part)); assert.ok(part.length <= n);
    }
    assert.equal(R.visiblePrefix(text, text.length), text);
  }
});

test("continuous arrivals cannot postpone the stalled-frame watchdog forever", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  reveal.update("x", true);
  for (let i = 2; i <= 5; i++) { c.advance(24, false); reveal.update("x".repeat(i), true); }
  c.advance(4, false);
  assert.equal(reveal.read(0, 100), 5);
  assert.equal(c.pending(), 0);
});

test("thinking window follows revealed text and preserves grapheme boundaries", () => {
  const text = "x".repeat(2100) + "👩‍💻" + "y".repeat(2100);
  assert.equal(R.visibleWindow(text, 100, 2000), "x".repeat(100));
  assert.equal(R.visibleWindow(text, 3000, 2000).length, 2000);
  const count = 2100 + 2 + 2000;
  const window = R.visibleWindow(text, count, 2000);
  assert.ok(window.startsWith("👩‍💻"));
  assert.equal(R.visibleWindow(text, text.length, 2000), "y".repeat(2000));
});

test("virtualized remount shows received text immediately and smooths only new arrivals", () => {
  const c = revealClock(), reveal = new StreamReveal(false, c.api);
  const received = "x".repeat(3000);
  assert.equal(reveal.read(0, received.length), received.length);
  reveal.update(received, true);
  assert.equal(c.pending(), 0);
  reveal.update(received + "y".repeat(100), true);
  c.advance(16);
  assert.ok(reveal.read(0, 3100) > 3000 && reveal.read(0, 3100) < 3100);
});

test("repeated identical snapshots do not postpone the reveal deadline", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  const text = "x".repeat(100);
  reveal.update(text, true);
  for (let i = 0; i < 15; i++) { c.advance(16); reveal.update(text, true); }
  assert.equal(reveal.read(0, 100), 100);
  assert.equal(c.pending(), 0);
});

test("observed OMP bursts avoid emptying the queue too early and bound per-frame jumps", () => {
  const { batches } = JSON.parse(readFileSync(join(ROOT, "npm", "fixtures", "omp-arrival-cadence.json"), "utf8"));
  const c = revealClock(), reveal = new StreamReveal(false, c.api);
  let text = "", index = 0, emptyFrames = 0, maxStep = 0, previous = 0;
  for (let now = 0; now < batches[batches.length - 1][0] + 500; now += 1000 / 60) {
    while (index < batches.length && batches[index][0] <= now) {
      text += "x".repeat(batches[index++][1]); reveal.update(text, true);
    }
    c.advance(1000 / 60);
    const visible = reveal.read(0, text.length);
    if (index > 1) {
      maxStep = Math.max(maxStep, visible - previous);
      if (visible === text.length && index < batches.length) emptyFrames++;
    }
    previous = visible;
  }
  // Fixed 80ms pacing on this trace had ~303 empty frames and jumps of 8.
  assert.ok(emptyFrames < 180, `empty frames: ${emptyFrames}`);
  assert.ok(maxStep <= 4, `largest jump: ${maxStep}`);
  assert.equal(reveal.read(0, text.length), text.length);
  assert.equal(c.pending(), 0);
});

test("a 200 tok/s burst stream advances every frame instead of landing whole batches", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  const burst = "汉".repeat(100);
  const burstFrames = Math.round(144 / (1000 / 60));
  let text = "", maxStep = 0, previous = 0, maxLag = 0, dumped = 0;
  for (let b = 0; b < 20; b++) {
    text += burst; reveal.update(text, true);
    for (let f = 0; f < burstFrames; f++) {
      c.advance(1000 / 60);
      const visible = reveal.read(0, text.length);
      const step = visible - previous;
      maxStep = Math.max(maxStep, step); if (step >= 20) dumped++;
      maxLag = Math.max(maxLag, text.length - visible);
      previous = visible;
    }
  }
  for (let f = 0; f < 20; f++) c.advance(1000 / 60);
  assert.equal(dumped, 0, `frames revealing 20+ characters at once: ${dumped}`);
  assert.ok(maxStep <= 20, `largest single-frame step: ${maxStep}`);
  assert.ok(maxLag >= 50, `reveal is not holding anything back: ${maxLag}`);
  assert.ok(maxLag <= 200, `largest backlog shown late: ${maxLag}`);
  assert.ok(maxLag < text.length / 4, `backlog relative to the stream: ${maxLag}/${text.length}`);
  assert.equal(reveal.read(0, text.length), text.length);
  assert.equal(c.pending(), 0);
  assert.equal(R.REVEAL_MAX_LAG_MS, 240);
});

test("markdown reshaping the rendered text does not dump the unrevealed backlog", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  const head = "x".repeat(300);
  reveal.update(head, true);
  c.advance(16);
  const before = reveal.read(0, head.length);
  assert.ok(before > 0 && before < head.length, `cursor mid-drain: ${before}`);
  reveal.update(head.slice(0, 150) + "y" + head.slice(151), true);
  const after = reveal.read(0, head.length);
  assert.ok(after >= before, `reshape must not hide text: ${before} -> ${after}`);
  assert.ok(after < head.length, `reshape must not reveal the backlog: ${after}`);
  c.advance(1000);
  assert.equal(reveal.read(0, head.length), head.length);
});

test("thinking window drops whole rows from the revealed cursor, not from the received tail", () => {
  const text = "1234567890\n".repeat(300);
  const end = 2000, limit = 1000;
  const windowed = R.visibleLineWindow(text, end, limit);
  assert.equal(windowed.truncated, true);
  assert.equal(windowed.text, text.slice(1001, end));
  assert.ok(windowed.text.length <= limit);
  const early = R.visibleLineWindow(text, 600, limit);
  assert.deepEqual(early, { text: text.slice(0, 600), truncated: false });
  const past = R.visibleLineWindow(text, text.length, limit);
  assert.ok(past.text.length <= limit);
  assert.ok(past.text.startsWith("1234567890\n"));
});

test("a cancelled drain resumes on the next identical snapshot instead of freezing", () => {
  const c = revealClock(), reveal = new StreamReveal(true, c.api);
  reveal.update("x".repeat(200), true);
  c.advance(32);
  const before = reveal.read(0, 200);
  assert.ok(before > 0 && before < 200, `mid-drain: ${before}`);
  reveal.cancel();
  assert.equal(reveal.read(0, 200), before, "cancel keeps the cursor for a real unmount");
  reveal.update("x".repeat(200), true);
  for (let i = 0; i < 6; i++) c.advance(1000 / 60);
  const resumed = reveal.read(0, 200);
  assert.ok(resumed > before && resumed < 200, `resumed: ${before} -> ${resumed}`);
  for (let i = 0; i < 7; i++) c.advance(1000 / 60);
  assert.equal(reveal.read(0, 200), 200, "cleared by the original deadline");
  assert.equal(c.pending(), 0);
});

test("reused grapheme reader preserves exact boundaries while the cursor moves both ways", () => {
  const text = "A👩‍💻e\u0301🇨🇳你好".repeat(30);
  const boundaries = [0, ...Array.from(new Intl.Segmenter(undefined, { granularity: "grapheme" }).segment(text), (s) => s.index + s.segment.length)];
  const reader = R.createVisibleTextReader(text);
  for (const count of [...Array.from({ length: text.length + 1 }, (_, i) => i)].reverse()) {
    const end = boundaries.filter((n) => n <= count).at(-1);
    const start = boundaries.filter((n) => n <= Math.max(0, end - 20)).at(-1);
    assert.equal(reader.prefix(count), text.slice(0, end));
    assert.equal(reader.window(count, 20), text.slice(start, end));
  }
});

// --------------------------------------------------------------- throttle —

test("continuous arrivals keep a fixed deadline and publish only the latest text", () => {
  const c = throttleClock(), t = new ThrottledText("", c.api);
  const seen = [];
  t.subscribe(() => seen.push(t.read()));
  t.update("a", 32); c.advance(8);
  t.update("ab", 32); c.advance(8);
  t.update("abc", 32); c.advance(16);
  assert.deepEqual(seen, ["abc"]);
  t.update("abcd", 32); c.advance(32);
  assert.deepEqual(seen, ["abc", "abcd"]);
  assert.equal(c.pending(), 0);
});

test("completion, longer replacement and truncation cancel obsolete tails", () => {
  const c = throttleClock(), t = new ThrottledText("initial", c.api);
  t.update("initial pending", 128);
  assert.equal(t.bypass("a different and longer replacement", 128), true);
  t.update("a different and longer replacement", 128);
  assert.equal(t.read(), "a different and longer replacement");
  t.update("短", 128);
  t.update("短🙂", 0);
  c.advance(200);
  assert.equal(t.read(), "短🙂");
  assert.equal(c.pending(), 0);
});

test("parse interval backs off with the measured commit cost, bounded", () => {
  assert.equal(R.nextParseInterval(R.streamParseInterval(2000), 6), 32);
  assert.equal(R.nextParseInterval(R.streamParseInterval(20000), 10), 128);
  assert.equal(R.nextParseInterval(32, 30), 60);
  assert.equal(R.nextParseInterval(64, 45), 90);
  assert.equal(R.nextParseInterval(128, 400), 160);
  assert.equal(R.nextParseInterval(32, 0), 32);
  assert.equal(R.nextParseInterval(32, Number.NaN), 32);
});

test("idle arrivals publish immediately; cancel/remount preserves the pending deadline", () => {
  const c = throttleClock(), t = new ThrottledText("", c.api);
  c.advance(200); t.update("a", 32);
  assert.equal(t.read(), "a");
  t.update("ab", 32); t.cancel();
  assert.equal(c.pending(), 0);
  c.advance(16); t.update("ab", 32); c.advance(16);
  assert.equal(t.read(), "ab");
});

// ----------------------------------------------------------------- cache —

function countingHighlighter() {
  let calls = 0;
  const fn = (code, lang) => { calls++; return `<b class="${lang}">${code}</b>`; };
  return { fn, calls: () => calls };
}

test("unchanged code reuses highlighting; changed code or language invalidates it", () => {
  const h = countingHighlighter();
  const cache = R.createCachedHighlighter(h.fn);
  assert.equal(cache.codeHtml("let x = 1", "js"), '<b class="js">let x = 1</b>');
  assert.equal(cache.codeHtml("let x = 1", "js"), '<b class="js">let x = 1</b>');
  assert.equal(cache.codeHtml("let x = 1", "js"), '<b class="js">let x = 1</b>');
  assert.equal(h.calls(), 1);
  assert.equal(cache.codeHtml("let x = 2", "js"), '<b class="js">let x = 2</b>');
  assert.equal(cache.codeHtml("let x = 2", "ts"), '<b class="ts">let x = 2</b>');
  assert.equal(h.calls(), 3);
});

test("cache eviction and oversized blocks keep the bounds", () => {
  const h = countingHighlighter();
  const one = R.createCachedHighlighter(h.fn, { entries: 1, characters: 1000 });
  one.codeHtml("a", "js");
  one.codeHtml("b", "js");
  one.codeHtml("a", "js");
  assert.equal(h.calls(), 3);
  const capped = R.createCachedHighlighter(h.fn, { entries: 8, characters: 10 });
  const big = "x".repeat(200);
  capped.codeHtml(big, "js");
  capped.codeHtml(big, "js");
  assert.equal(h.calls(), 5, "oversized keys are rejected, never cached");
  const disabled = R.createCachedHighlighter(h.fn, { entries: 0 });
  disabled.codeHtml("same", "js");
  disabled.codeHtml("same", "js");
  assert.equal(h.calls(), 7, "entries: 0 disables the cache");
});

test("non-highlightable results are returned as null and not cached", () => {
  let calls = 0;
  const cache = R.createCachedHighlighter((code, lang) => { calls++; return lang === "known" ? "<b>x</b>" : null; });
  assert.equal(cache.codeHtml("code", "unknown"), null);
  assert.equal(cache.codeHtml("code", "unknown"), null);
  assert.equal(calls, 2);
  assert.equal(cache.codeHtml("code", "known"), "<b>x</b>");
  assert.equal(cache.codeHtml("code", "known"), "<b>x</b>");
  assert.equal(calls, 3);
});

// ---------------------------------------------------------------- walker —

function stubEnv() {
  return {
    createElement() {
      const n = { nodeType: 1, tagName: "SPAN", className: "", childNodes: [], attrs: {} };
      n.setAttribute = (k, v) => { n.attrs[k] = v; };
      return n;
    },
    replaceChild(parent, newNode, oldNode) {
      const i = parent.childNodes.indexOf(oldNode);
      parent.childNodes[i] = newNode;
    },
  };
}
const t = (value) => ({ nodeType: 3, nodeValue: value });
const e = (tag, children = []) => ({ nodeType: 1, tagName: tag.toUpperCase(), className: "", childNodes: children });

test("wrapRevealSpans assigns document-order offsets and a matching plan text", () => {
  const tree = e("div", [
    e("p", [t("Hello ")]),
    e("p", [t("bo"), e("strong", [t("ld")])]),
  ]);
  const plan = R.wrapRevealSpans(tree, stubEnv());
  assert.equal(plan.text, "Hello bold");
  assert.deepEqual(plan.spans.map((s) => [s.start, s.length]), [[0, 6], [6, 2], [8, 2]]);
  for (const s of plan.spans) assert.equal(s.node.attrs["data-stream-start"], String(s.start));
});

test("text under non-whitelisted parents is neither wrapped nor counted", () => {
  const tree = e("div", [
    e("ul", [t("bare in ul"), e("li", [t("in li")])]),
    e("table", [e("tr", [e("td", [t("cell")])])]),
  ]);
  const plan = R.wrapRevealSpans(tree, stubEnv());
  assert.equal(plan.text, "in licell");
  assert.deepEqual(plan.spans.map((s) => [s.start, s.length]), [[0, 5], [5, 4]]);
});

test("opaque subtrees are skipped; fenced code text participates", () => {
  // Build the codewrap shape mdLite emits: label span + pre>code.
  const label = e("span", [t("js")]);
  label.className = "codeLang";
  const codewrap = e("div", [e("button", [t("copy")]), label, e("pre", [e("code", [t("let x = 1")])])]);
  codewrap.className = "codewrap";
  const root = e("div", [codewrap, e("p", [t("tail")])]);
  const plan = R.wrapRevealSpans(root, stubEnv());
  // Button text is under a non-whitelisted parent; the label is opaque;
  // only the code text and the paragraph participate.
  assert.equal(plan.text, "let x = 1tail");
  assert.deepEqual(plan.spans.map((s) => [s.start, s.length]), [[0, 9], [9, 4]]);
});

test("spans drive per-range text from a StreamReveal cursor", () => {
  const c = revealClock();
  const reveal = new StreamReveal(true, c.api);
  const tree = e("div", [e("p", [t("one ")]), e("p", [t("two")])]);
  const plan = R.wrapRevealSpans(tree, stubEnv());
  for (const s of plan.spans) {
    // Each span keeps a reader over its own text, like the UI binding does.
    const reader = R.createVisibleTextReader(plan.text.slice(s.start, s.start + s.length));
    s.node.textContent = "";
    reveal.subscribe(s.start, s.length, () => {
      s.node.textContent = reader.prefix(reveal.read(s.start, s.length));
    });
  }
  reveal.update(plan.text, true);
  for (let i = 0; i < 20; i++) c.advance(16);
  assert.equal(plan.spans[0].node.textContent, "one ");
  assert.equal(plan.spans[1].node.textContent, "two");
});
