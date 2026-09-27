// Damon streaming reveal pipeline — pacing, parse throttling, cached code
// highlighting and the reveal span planner for the bundled web UI.
//
// The core cursor/throttle algorithms are ported from desktop-cc-gui
// (https://github.com/zhukunpenglinyutong/desktop-cc-gui, MIT):
//   src/features/chat/components/stream-reveal.ts
//   src/hooks/throttled-text.ts
//   src/features/chat/components/cached-highlight.ts
// The DOM walker and the string-based highlight cache are adaptations of the
// same designs to this file's dependency-free classic-script constraints.
//
// Exposed as globalThis.DamonStreamReveal (same pattern as relay-client.js)
// so the Node test-suite can load it exactly like a browser would.
(function (global) {
  "use strict";

  // ---------------------------------------------------------------- clock —

  /** Injectable clock so tests can drive frames and timers deterministically.
   *  The browser default touches performance/rAF lazily — loading this file
   *  under Node (no rAF) must not throw. */
  const browserClock = {
    now: () => performance.now(),
    frame: (cb) => requestAnimationFrame(cb),
    cancelFrame: (id) => cancelAnimationFrame(id),
    timeout: (cb, ms) => setTimeout(cb, ms),
    clearTimeout: (id) => clearTimeout(id),
  };

  // -------------------------------------------------------- StreamReveal —

  /** Upper bound on how long received text may stay hidden. Every burst is
   *  drained by this deadline, so the display never lags more than this. */
  const REVEAL_MAX_LAG_MS = 240;
  /** Shortest spread for one burst: below this the drain reads as a jump, not
   *  as flowing text (the old fixed 80ms drain is the floor, not the rule). */
  const MIN_LAG_MS = 80;
  /** A backlog this large is a wholesale dump (tab restore, session switch,
   *  first paint of a long snapshot), not a stream: show it at once instead of
   *  animating for seconds. */
  const MAX_HOLDBACK = 600;
  /** No frame for this long means rAF is suspended (occluded window, background
   *  tab): settle rather than leaving text half-revealed behind the scenes. */
  const STALL_MS = 100;

  /** Presentation-only cursor: the accumulated message text always keeps the
   *  complete content; this class only paces what is shown.
   *
   *  The cursor is anchored to the TAIL of the text — a `holdback` of trailing
   *  characters still hidden — instead of an absolute prefix index. Markdown
   *  re-parsing rewrites the rendered text constantly (syntax markers are
   *  consumed: `# `, `- `, backticks, `**`, link targets), so a prefix cursor
   *  has to reset whenever the text changed shape, and at high token rates
   *  that reset dumped the whole unrevealed backlog in a single frame
   *  (measured upstream: 20–62 chars/frame at 200 tok/s, up to 140 at 400). */
  class StreamReveal {
    constructor(live, clock) {
      this.clock = clock || browserClock;
      this.text = "";
      /** Published visible prefix of `text`; Infinity = historical instance
       *  that has not received a live update yet (everything is visible). */
      this.visible = live ? 0 : Infinity;
      /** Trailing characters already rendered but not published yet. */
      this.holdback = 0;
      /** Wall-clock time by which the current holdback must be fully shown. */
      this.deadline = 0;
      /** Measured arrival: cadence (ms between text changes) and rate
       *  (characters per ms). A burst is spread over the cadence that produced
       *  it; a rate-matching drain keeps a long fast stream from accumulating
       *  unbounded lag. */
      this.cadence = 80;
      this.rate = 0;
      this.lastArrival = undefined;
      this.frame = undefined;
      this.timer = undefined;
      this.lastTick = 0;
      this.listeners = new Set();
    }
    read(start, length) {
      return Math.max(0, Math.min(length, this.visible - start));
    }
    subscribe(start, length, notify) {
      const listener = { start, end: start + length, notify };
      this.listeners.add(listener);
      return () => { this.listeners.delete(listener); };
    }
    publish(next) {
      const previous = this.visible;
      this.visible = next;
      if (next === previous) return;
      for (const listener of this.listeners) {
        // Growing notifies only listeners the cursor crossed this frame —
        // parsing, highlighting and settled spans stay outside the loop.
        if (next < previous || (listener.end > previous && listener.start < next)) listener.notify();
      }
    }
    update(text, animate) {
      if (text === this.text) {
        if (!animate) {
          this.finish();
          return;
        }
        // A pending drain must survive a cancelled frame loop (teardown on
        // remount/session switch). Re-arm without moving the deadline, so an
        // unchanged snapshot can never leave half-revealed text on screen.
        if (this.holdback > 0) this.run(this.clock.now());
        return;
      }
      const now = this.clock.now();
      const previous = this.text;
      this.text = text;
      if (!animate || this.visible === Infinity || isReplacement(previous, text)) {
        this.finish();
        return;
      }
      // The rendered text may change shape without being a literal extension
      // (markdown consumed `**`, `# `, a link target…). The new characters are
      // what needs revealing — not the whole snapshot — so track the cursor
      // relative to the end and let a reshape move it with the text.
      const gained = text.length - previous.length;
      this.holdback = Math.max(0, this.holdback + gained);
      if (this.holdback > MAX_HOLDBACK) {
        this.finish();
        return;
      }
      if (gained > 0 && this.lastArrival !== undefined) {
        const gap = now - this.lastArrival;
        // Sub-frame gaps are part of one provider batch, not a cadence.
        if (gap >= 16) {
          this.cadence = this.cadence * 0.5 + Math.min(gap, 220) * 0.5;
          this.rate = this.rate > 0 ? this.rate * 0.5 + (gained / gap) * 0.5 : gained / gap;
        }
      }
      this.lastArrival = now;
      // Text may only be pulled back by text that actually disappeared.
      if (this.text.length < this.visible) this.publish(this.text.length);
      if (this.holdback <= 0) {
        this.cancel();
        return;
      }
      // Never drain faster than the arrival cadence (that would empty the
      // queue and stall between bursts) nor faster than the measured rate can
      // feed (that would burn the whole burst in one frame); both bounded by
      // MAX_LAG so the display never lags more than that.
      const spread = this.rate > 0
        ? Math.max(this.cadence * 1.1, this.holdback / this.rate)
        : REVEAL_MAX_LAG_MS;
      this.deadline = now + Math.min(REVEAL_MAX_LAG_MS, Math.max(MIN_LAG_MS, spread));
      this.run(now);
    }
    /** Start (or keep) the single drain loop plus its stalled-frame watchdog.
     *  One continuous loop per burst beats restarting per commit: restarts
     *  recompute the pacing and produce uneven steps. */
    run(now) {
      if (this.frame === undefined) {
        this.lastTick = now;
        this.frame = this.clock.frame(() => this.tick());
      }
      if (this.timer === undefined) {
        const watchdog = () => {
          this.timer = undefined;
          const idle = this.clock.now() - this.lastTick;
          if (idle >= STALL_MS) {
            this.finish();
            return;
          }
          this.timer = this.clock.timeout(watchdog, STALL_MS - idle);
        };
        this.timer = this.clock.timeout(watchdog, STALL_MS);
      }
    }
    /** Drain at the rate that clears the holdback exactly at `deadline`. The
     *  release is fractional, so a small burst still lands one character at a
     *  time over its whole cadence instead of emptying in one frame. */
    tick() {
      this.frame = undefined;
      const now = this.clock.now();
      const remaining = this.deadline - now;
      if (this.holdback <= 0.5 || remaining <= 0) {
        this.holdback = 0;
        this.publish(this.text.length);
        this.cancel();
        return;
      }
      const dt = Math.max(0, Math.min(now - this.lastTick, remaining));
      this.lastTick = now;
      this.holdback = Math.max(0, this.holdback - (this.holdback * dt) / remaining);
      // Never step backwards inside the loop; only shrinking text may do that
      // (handled by update()).
      const target = this.text.length - Math.round(this.holdback);
      if (target > this.visible) this.publish(target);
      this.frame = this.clock.frame(() => this.tick());
    }
    finish() {
      this.cancel();
      this.holdback = 0;
      this.publish(this.text.length);
    }
    cancel() {
      if (this.frame !== undefined) this.clock.cancelFrame(this.frame);
      if (this.timer !== undefined) this.clock.clearTimeout(this.timer);
      this.frame = undefined;
      this.timer = undefined;
    }
  }

  /** A wholesale swap shares almost nothing with what came before: replaying a
   *  different message character by character is not "smooth", it is wrong.
   *  Only the head can diverge during markdown re-parsing (a construct at the
   *  start of the text completing), so a generous head threshold is enough. */
  function isReplacement(previous, next) {
    if (!previous || !next) return false;
    const limit = Math.min(previous.length, next.length);
    let common = 0;
    while (common < limit && previous[common] === next[common]) common += 1;
    return common < Math.min(24, limit * 0.5);
  }

  // ---------------------------------------------------- grapheme safety —

  const segmenter = typeof Intl.Segmenter === "function"
    ? new Intl.Segmenter(undefined, { granularity: "grapheme" }) : null;

  /** Never show half an emoji, combining sequence, or surrogate pair. */
  function visiblePrefix(text, count) {
    return createVisibleTextReader(text).prefix(count);
  }

  /** Window follows the revealed cursor, not the received tail, so a large
   *  thinking chunk cannot hide all content while the cursor catches up. */
  function visibleWindow(text, count, limit) {
    return createVisibleTextReader(text).window(count, limit);
  }

  /** Same contract as `visibleWindow`, but the window starts on a LINE
   *  boundary. Pre-wrapped thinking text re-wraps when its first character
   *  changes, so a character cut makes the whole paragraph shift; dropping
   *  whole rows keeps the block's line count stable while the cursor drains. */
  function visibleLineWindow(text, count, limit) {
    return createVisibleTextReader(text).lineWindow(count, limit);
  }

  /** One segmentation handle per text snapshot, shared by all reveal frames.
   *  Boundaries use the platform grapheme algorithm, including joined emoji
   *  and combining marks. Without Intl.Segmenter everything shows at once —
   *  splitting a grapheme is worse than not pacing it. */
  function createVisibleTextReader(text) {
    let segments;
    const boundary = (count) => {
      if (count >= text.length) return text.length;
      if (count <= 0) return 0;
      if (segments === undefined && segmenter) segments = segmenter.segment(text);
      return (segments && segments.containing) ? (segments.containing(count) || {}).index ?? text.length : text.length;
    };
    return {
      prefix: (count) => text.slice(0, boundary(count)),
      window(count, limit) {
        const end = boundary(count);
        if (end <= limit) return text.slice(0, end);
        const start = boundary(end - limit);
        // Without grapheme support, show complete text rather than split emoji.
        return text.slice(start > end - limit ? 0 : start, end);
      },
      lineWindow(count, limit) {
        const end = boundary(count);
        if (end <= limit) return { text: text.slice(0, end), truncated: false };
        const cut = end - limit;
        const newline = text.indexOf("\n", cut);
        // No line boundary inside the window (one enormous line) or the only
        // newline after the cut is past the revealed cursor: fall back to a
        // grapheme-safe character cut — there is no row to drop as a whole.
        const start = newline === -1 || newline >= end ? boundary(cut) : newline + 1;
        return { text: text.slice(start, end), truncated: true };
      },
    };
  }

  // ----------------------------------------------------- parse throttling —

  /** Short replies can be parsed more often; large documents need breathing
   *  room for input, layout and reveal frames between full Markdown parses. */
  function streamParseInterval(length) {
    return length <= 4000 ? 32 : length <= 16000 ? 64 : 128;
  }

  /** A live commit competes with the reveal frames for the main thread: at
   *  200 tok/s the base cadence asks for 30 full-document parses per second,
   *  and a parse that overruns the frame budget starves presentation (the
   *  text then lands in uneven chunks). A commit that costs more than the
   *  base interval gets proportionally more room next time — a bounded
   *  backoff that keeps the parse pipeline near half the wall clock instead
   *  of as fast as possible. */
  function nextParseInterval(base, commitMs) {
    if (!(commitMs > 0)) return base;
    return Math.min(160, Math.max(base, Math.round(commitMs * 2)));
  }

  /** Committed input and displayed text have a single owner. Timers publish
   *  the latest input, never a captured older snapshot. */
  class ThrottledText {
    constructor(value, clock) {
      this.clock = clock || browserClock;
      this.input = value;
      this.visible = value;
      this.lastEmit = this.clock.now();
      this.timer = undefined;
      this.listeners = new Set();
    }
    read() { return this.visible; }
    subscribe(notify) {
      this.listeners.add(notify);
      return () => { this.listeners.delete(notify); };
    }
    bypass(value, interval) {
      return interval <= 0 || !value.startsWith(this.input);
    }
    update(value, interval) {
      const immediate = this.bypass(value, interval);
      this.input = value;
      this.cancel();
      if (value === this.visible) return;
      const wait = interval - (this.clock.now() - this.lastEmit);
      if (immediate || wait <= 0) this.publish();
      else this.timer = this.clock.timeout(() => {
        this.timer = undefined;
        this.publish();
      }, wait);
    }
    publish() {
      this.lastEmit = this.clock.now();
      if (this.visible === this.input) return;
      this.visible = this.input;
      for (const notify of this.listeners) notify();
    }
    cancel() {
      if (this.timer !== undefined) this.clock.clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  // ------------------------------------------------ cached highlighting —

  /** Bounded LRU over code-fence highlighting. Keep re-parsing the complete
   *  document (later reference definitions, tables and list context must stay
   *  correct); reuse only the highlighted code HTML.
   *
   *  Adaptation of ccgui's HAST-node cache for a string renderer: the value is
   *  the highlighted HTML string splice, the key is [language, source code].
   *  `highlightFn(code, lang)` returns the HTML string, or null/undefined when
   *  the block is not highlightable (no library, unknown language) — misses of
   *  that kind are not cached. */
  function createCachedHighlighter(highlightFn, limits) {
    const lim = limits || { entries: 32, characters: 256000 };
    const cache = new Map();
    let characters = 0;
    return {
      codeHtml(code, lang) {
        const key = JSON.stringify([lang === undefined || lang === null ? null : lang, code]);
        const cached = cache.get(key);
        if (cached !== undefined) {
          cache.delete(key);
          cache.set(key, cached);
          return cached;
        }
        const html = highlightFn(code, lang);
        if (html === null || html === undefined) return null;
        if (key.length <= lim.characters && lim.entries > 0) {
          while (cache.size >= lim.entries || characters + key.length > lim.characters) {
            const oldest = cache.keys().next().value;
            if (oldest === undefined) break;
            cache.delete(oldest);
            characters -= oldest.length;
          }
          cache.set(key, html);
          characters += key.length;
        }
        return html;
      },
    };
  }

  // -------------------------------------------------------- reveal plan —

  /** Element tags whose direct text runs participate in the reveal: offsets
   *  are only assigned to text the cursor can actually cross. */
  const REVEAL_TEXT_PARENTS = new Set([
    "p", "span", "strong", "em", "del", "a", "code",
    "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "blockquote", "div",
  ]);

  /** Walk rendered DOM and wrap every revealable text run in
   *  `<span data-stream-start=N>`; returns { text, spans } where `text` is the
   *  concatenation of the wrapped runs (the coordinate space the StreamReveal
   *  cursor runs over) and `spans` binds each wrapper to its range.
   *
   *  `env` ({ createElement, replaceChild }) is injectable so Node tests can
   *  drive the walker over plain object trees; the default uses the live DOM. */
  function wrapRevealSpans(root, env) {
    const doc = env || {
      createElement: (tag) => document.createElement(tag),
      replaceChild: (parent, newNode, oldNode) => parent.replaceChild(newNode, oldNode),
    };
    // Opaque subtrees keep their exact DOM shape (the floating fence-language
    // label never participates in the cursor).
    const isOpaque = (el) => {
      const cls = typeof el.className === "string" ? el.className : "";
      return /\bcodeLang\b/.test(cls);
    };
    let offset = 0;
    const parts = [];
    const spans = [];
    const walk = (parent, parentAllowed) => {
      const children = Array.from(parent.childNodes || []);
      for (const node of children) {
        if (node.nodeType === 3) {
          const text = node.nodeValue || "";
          if (!text || !parentAllowed) continue;
          const start = offset;
          offset += text.length;
          parts.push(text);
          const span = doc.createElement("span");
          span.setAttribute("data-stream-start", String(start));
          span.textContent = text;
          doc.replaceChild(parent, span, node);
          spans.push({ node: span, start, length: text.length });
        } else if (node.nodeType === 1) {
          if (isOpaque(node)) continue;
          const tag = String(node.tagName || "").toLowerCase();
          walk(node, REVEAL_TEXT_PARENTS.has(tag));
        }
      }
    };
    walk(root, true);
    return { text: parts.join(""), spans };
  }

  global.DamonStreamReveal = {
    REVEAL_MAX_LAG_MS,
    browserClock,
    StreamReveal,
    visiblePrefix,
    visibleWindow,
    visibleLineWindow,
    createVisibleTextReader,
    streamParseInterval,
    nextParseInterval,
    ThrottledText,
    createCachedHighlighter,
    wrapRevealSpans,
  };
})(typeof globalThis !== "undefined" ? globalThis : this);
