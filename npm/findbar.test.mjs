// Find-bar pipeline tests — the pure find/highlight module extracted from
// src/ui.html (markers: "damon-find pure module"). Mirrors the stub-DOM
// approach of stream-reveal.test.mjs: stub nodes/element trees plus a stub
// `doc` factory, so no real browser is needed.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

// The module lives inline in the single-file app — slice it out the same
// way a browser would load it, then eval so the declarations become globals.
const html = readFileSync(join(ROOT, "src", "ui.html"), "utf8");
const BEGIN = "// ==== damon-find pure module";
const END = "// ==== end damon-find pure module";
const a = html.indexOf(BEGIN), b = html.indexOf(END);
assert.ok(a !== -1 && b > a, "find module markers present in src/ui.html");
(0, eval)(html.slice(a, b));
const { findMatches, clearHighlights, applyHighlights } = globalThis;

// Stub nodes mimic the parts of the real DOM the module relies on —
// notably mutation via replaceChild/insertBefore and nextSibling, NOT
// array methods on childNodes (a live NodeList has none).
function makeElement(tag) {
  const n = { nodeType: 1, tagName: tag.toUpperCase(), className: "", childNodes: [] };
  n.appendChild = (c) => { n.childNodes.push(c); c.parentNode = n; return c; };
  n.replaceChild = (nu, old) => {
    const i = n.childNodes.indexOf(old);
    n.childNodes[i] = nu;
    nu.parentNode = n;
    old.parentNode = null;
    return old;
  };
  n.insertBefore = (nu, ref) => {
    const i = ref ? n.childNodes.indexOf(ref) : n.childNodes.length;
    n.childNodes.splice(i, 0, nu);
    nu.parentNode = n;
    return nu;
  };
  Object.defineProperty(n, "nextSibling", {
    get() {
      const s = n.parentNode ? n.parentNode.childNodes : [];
      return s[s.indexOf(n) + 1] || null;
    },
  });
  return n;
}
const e = (tag, children = []) => {
  const n = makeElement(tag);
  for (const c of children) { n.childNodes.push(c); c.parentNode = n; }
  return n;
};
const t = (value) => {
  const n = { nodeType: 3, nodeValue: value };
  Object.defineProperty(n, "nextSibling", {
    get() {
      const s = n.parentNode ? n.parentNode.childNodes : [];
      return s[s.indexOf(n) + 1] || null;
    },
  });
  return n;
};
const doc = { createElement: makeElement, createTextNode: t };
const text = (root) => {
  let s = "";
  (function walk(n) {
    for (const c of n.childNodes) {
      if (c.nodeType === 3) s += c.nodeValue;
      else walk(c);
    }
  })(root);
  return s;
};

// ----------------------------------------------------------- findMatches —

test("findMatches is case-insensitive, non-overlapping and node-mapped", () => {
  const nodes = [t("The Needle"), t(" in the "), t("hay, NEEDLE again")];
  assert.deepEqual(findMatches(nodes, "needle"), [
    { node: nodes[0], start: 4, end: 10 },
    { node: nodes[2], start: 5, end: 11 },
  ]);
});

test("findMatches returns nothing for empty or absent queries", () => {
  const nodes = [t("nothing here")];
  assert.deepEqual(findMatches(nodes, ""), []);
  assert.deepEqual(findMatches(nodes, "zzz"), []);
});

test("a match spanning inline element boundaries maps onto each node", () => {
  const n0 = t("abc"), n1 = t("def");
  assert.deepEqual(findMatches([n0, n1], "abcdef"), [
    { node: n0, start: 0, end: 3 },
    { node: n1, start: 0, end: 3 },
  ]);
});

// ------------------------------------------------------ applyHighlights —

test("applyHighlights splits text nodes into marks and preserves content", () => {
  const root = e("div", [e("p", [t("a needle b needle c")])]);
  const marks = applyHighlights(root, "needle", doc);
  assert.equal(marks.length, 2);
  const p = root.childNodes[0];
  assert.deepEqual(
    p.childNodes.map(n => [n.nodeType, n.nodeType === 1 ? n.className : n.nodeValue]),
    [
      [3, "a "], [1, "fmark"], [3, " b "], [1, "fmark"], [3, " c"],
    ]);
  assert.deepEqual(marks.map(m => m.childNodes[0].nodeValue), ["needle", "needle"]);
  assert.equal(text(root), "a needle b needle c");
});

test("re-applying replaces previous marks instead of nesting them", () => {
  const root = e("div", [e("p", [t("needle one")])]);
  applyHighlights(root, "needle", doc);
  const again = applyHighlights(root, "one", doc);
  assert.equal(again.length, 1);
  // old mark unwrapped to plain text, then "one" split off and wrapped
  assert.deepEqual(
    root.childNodes[0].childNodes.map(n => n.nodeType === 1 ? "M" : n.nodeValue),
    ["needle", " ", "M"]);
  assert.equal(text(root), "needle one");
});

test("clearHighlights restores plain text nodes", () => {
  const root = e("div", [e("p", [t("x needle y")])]);
  applyHighlights(root, "needle", doc);
  clearHighlights(root, doc);
  const p = root.childNodes[0];
  assert.ok(p.childNodes.every(n => n.nodeType === 3));
  assert.equal(p.childNodes.map(n => n.nodeValue).join(""), "x needle y");
});

test("the active mark (className 'fmark active') is unwrapped too", () => {
  // Regression: an exact className === "fmark" check left every mark that
  // currently carried the cycling "active" class in the DOM after close.
  const root = e("div", [e("p", [t("needle a needle b")])]);
  const marks = applyHighlights(root, "needle", doc);
  marks[1].className = "fmark active"; // what stepFind's classList.add yields
  clearHighlights(root, doc);
  const p = root.childNodes[0];
  assert.ok(p.childNodes.every(n => n.nodeType === 3));
  assert.equal(p.childNodes.map(n => n.nodeValue).join(""), "needle a needle b");
});

test("buttons, marks, scripts and hidden subtrees are never wrapped", () => {
  const hiddenBtn = e("button", [t("needle")]);
  hiddenBtn.hidden = true;
  const root = e("div", [
    e("button", [t("needle")]),          // skip: controls aren't content
    e("script", [t("needle")]),          // skip: code
    e("span", [t("needle here")]),       // real hit
    hiddenBtn,                           // skip: hidden
  ]);
  const marks = applyHighlights(root, "needle", doc);
  assert.equal(marks.length, 1);
  assert.equal(marks[0].childNodes[0].nodeValue, "needle");
});

test("multiple nodes per parent keep sibling indexes intact after splits", () => {
  const root = e("div", [
    e("p", [t("first needle"), t("second needle"), t("third needle")]),
    e("p", [t("fourth needle")]),
  ]);
  const marks = applyHighlights(root, "needle", doc);
  assert.equal(marks.length, 4);
  assert.equal(text(root), "first needlesecond needlethird needlefourth needle");
  const p0 = root.childNodes[0];
  assert.deepEqual(p0.childNodes.map(n => n.nodeType === 1 ? "M" : n.nodeValue), [
    "first ", "M", "second ", "M", "third ", "M",
  ]);
});

test("a query matching across adjacent nodes wraps every touched node", () => {
  const root = e("div", [e("p", [t("abc"), e("strong", [t("def")]), t("ghi")])]);
  const marks = applyHighlights(root, "abcdefghi", doc);
  assert.equal(marks.length, 3);
  assert.equal(text(root), "abcdefghi");
  assert.deepEqual(
    root.childNodes[0].childNodes.map(n => n.nodeType === 1 ? "M" : n.nodeValue),
    ["M", "M", "M"]);
});

test("one text node becomes three correctly ordered pieces via replaceChild/insertBefore", () => {
  // Regression: the module once used childNodes.splice, which a real
  // browser's live NodeList does not implement — first keystroke threw.
  const p = e("p", [t("a needle c needle e")]);
  const marks = applyHighlights(p, "needle", doc);
  assert.equal(marks.length, 2);
  assert.deepEqual(
    p.childNodes.map(n => n.nodeType === 1 ? "M" : n.nodeValue),
    ["a ", "M", " c ", "M", " e"]);
  // Each mark's nextSibling is the text that follows it — the ordering
  // insertBefore relies on.
  assert.equal(marks[0].nextSibling.nodeValue, " c ");
  assert.equal(marks[1].nextSibling.nodeValue, " e");
  assert.equal(text(p), "a needle c needle e");
});
