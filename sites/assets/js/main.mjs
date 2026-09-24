// Damon intro site — i18n, copy buttons, 3D architecture scene.

/* ------------------------------ i18n ------------------------------ */

const I18N = {
  ko: {
    "meta.title": "Damon — 데몬 하나. 모든 코딩 에이전트.",
    "nav.features": "기능", "nav.arch": "구조", "nav.quick": "시작하기",
    "nav.chan": "채널", "nav.eco": "생태계",
    "hero.badge": "Rust 기반 로컬 에이전트 컨트롤 데몬",
    "hero.title1": "데몬 하나.",
    "hero.title2": "모든 코딩 에이전트.",
    "hero.lead": "Damon은 Claude Code·Codex CLI·Oh My Pi를 네이티브 CLI로 구동하는 Rust 상주 데몬입니다. 모델·툴·인증은 에이전트 몫 — Damon은 세션, 권한 릴레이, 검색 가능한 히스토리, 채팅 채널, 원격 접근을 소유합니다.",
    "hero.install.label": "설치",
    "hero.cta.quick": "퀵스타트",
    "hero.cta.gh": "GitHub에서 보기",
    "hero.hint": "드래그로 회전 · 스크롤로 확대 · 노드 클릭",
    "feat.kicker": "왜 Damon인가",
    "feat.title": "에이전트는 그대로, 제어는 한 곳에서",
    "feat.lead": "이미 에이전트 CLI를 구독 중이라면 설정할 게 하나도 없습니다. Damon은 설치된 CLI를 감지해 로그인과 툴을 그대로 물려받습니다.",
    "f1t": "설정 없는 백엔드",
    "f1d": "PATH의 claude·codex·omp를 자동 등록합니다. 다른 에이전트도 <code>[backends.X]</code> 블록 하나면 연결됩니다.",
    "f2t": "권한은 당신 채널로",
    "f2d": "에이전트의 권한 요청을 웹 UI·CLI·텔레그램/디스코드/슬랙의 <code>allow</code>/<code>deny</code> 답장으로 릴레이합니다.",
    "f3t": "검색 가능한 히스토리",
    "f3d": "모든 대화가 SQLite + FTS5에 기록됩니다. <code>damon search \"error timeout\"</code>으로 전체 이력 전문 검색.",
    "f4t": "채팅 채널 내장",
    "f4d": "Telegram·Discord·Slack 어댑터가 별도 바이너리로 제공됩니다. 채팅별 세션 자동 매핑, 스트리밍 응답.",
    "f5t": "시크릿은 Damon에 없다",
    "f5d": "에이전트가 자기 인증을 관리합니다. Damon config는 포트와 토큰 정도뿐.",
    "f6t": "어디서든 접근",
    "f6d": "<code>wss</code> 직접 서빙 또는 <code>damon-relay</code> 아웃바운드 터널 — X25519 + AES-256-GCM E2E 암호화.",
    "f7t": "상주하도록 설계",
    "f7d": "Rust 데몬 본체는 가볍게 유지됩니다. 에이전트 프로세스는 에이전트의 몫입니다.",
    "arch.kicker": "아키텍처",
    "arch.title": "하나의 API, 하나의 데몬",
    "arch.lead": "모든 클라이언트가 같은 JSON-RPC over WebSocket(<code>/ws</code>, 프로토콜 v2)에 붙고, 데몬이 에이전트 CLI를 서브프로세스로 구동합니다.",
    "arch.chip.web": "웹 UI", "arch.chip.desktop": "데스크톱 앱", "arch.chip.code": "직접 만든 코드",
    "arch.api": "단일 API — JSON-RPC over WebSocket · <code>/ws</code> · 프로토콜 v2",
    "arch.core.sub": "상주 데몬 · Rust",
    "arch.m1": "백엔드 레지스트리 — CLI 탐지·구동·재시작",
    "arch.m2": "세션 매니저 — 생명주기·권한 릴레이·취소",
    "arch.m3": "세션 저장소 — SQLite + FTS5",
    "arch.m4": "채널 브리지 / E2E 릴레이",
    "arch.agents.note": "모델·툴·구독 인증·컨텍스트는 전부 에이전트 소유",
    "arch.proto": "와이어 프로토콜 문서 →",
    "arch.integration": "클라이언트 연동 가이드 →",
    "quick.kicker": "퀵스타트",
    "quick.title": "3줄이면 충분합니다",
    "quick.install": "설치", "quick.run": "실행", "quick.check": "확인",
    "quick.c.npm": "npm (프리빌트 바이너리)",
    "quick.c.cargo": "crates.io",
    "quick.c.brew": "Homebrew tap",
    "quick.run.c1": "첫 실행 시 스타터 config 생성",
    "quick.run.c2": "어떤 백엔드가 설치됐는지 확인",
    "quick.service": "OS 서비스로 상주",
    "quick.service.c1": "launchd / systemd user / Task Scheduler",
    "quick.ui.t": "번들 웹 UI로 바로 대화",
    "quick.ui.p": "세션, 스트리밍, 권한 프롬프트까지 — 설치 불필요.",
    "chan.kicker": "채팅 채널",
    "chan.title": "채팅 앱이 곧 컨트롤 룸",
    "chan.lead": "각 채팅이 고유한 에이전트 세션에 매핑되고 응답은 스트리밍됩니다. tool 권한 요청은 <code>allow</code>/<code>deny</code> 답장으로 승인합니다.",
    "remote.kicker": "원격 접근",
    "remote.title": "어디서든, 안전하게",
    "remote.r1t": "Tailscale (권장)",
    "remote.r1d": "<code>ws://&lt;tailscale-ip&gt;:9470/ws</code>로 attach — WireGuard E2E, 데몬 설정 변경 없음.",
    "remote.r2t": "직접 TLS",
    "remote.r2d": "<code>tls_cert</code>/<code>tls_key</code>로 <code>wss</code> 서빙. 비루프백 바인드는 <code>auth_token</code> 없이 기동 거부.",
    "remote.r3t": "자체 릴레이",
    "remote.r3d": "공개 호스트의 <code>damon-relay</code>로 아웃바운드 연결 — 인바운드 포트 불필요. X25519 키 교환 → AES-256-GCM, 릴레이는 평문을 볼 수 없음.",
    "eco.kicker": "생태계",
    "eco.title": "모든 언어에서 한 줄로",
    "eco.npm.d": "npm 패키지에 포함된 무의존성 클라이언트.",
    "eco.py.d": "asyncio 기반 공식 클라이언트.",
    "eco.py.link": "python/ 디렉터리 →",
    "eco.rs.d": "crates.io의 코어 크레이트.",
    "foot.tag": "이름은 daemon의 말장난이자, 말 그대로 실제 아키텍처입니다.",
    "foot.license": "듀얼 라이선스 MIT 또는 Apache-2.0",
    "copy.aria": "명령 복사",
    "copy.done": "복사됨!",
    "tip.click": "클릭하면 해당 섹션으로 이동",
  },
  en: {
    "meta.title": "Damon — One daemon. Every coding agent.",
    "nav.features": "Features", "nav.arch": "Architecture", "nav.quick": "Quickstart",
    "nav.chan": "Channels", "nav.eco": "Ecosystem",
    "hero.badge": "Local agent-control daemon in Rust",
    "hero.title1": "One daemon.",
    "hero.title2": "Every coding agent.",
    "hero.lead": "Damon is a resident Rust daemon that drives Claude Code, Codex CLI, and Oh My Pi through their native CLIs. Model, tools, and auth belong to the agents — Damon owns sessions, permission relaying, searchable history, chat channels, and remote access.",
    "hero.install.label": "Install",
    "hero.cta.quick": "Quickstart",
    "hero.cta.gh": "View on GitHub",
    "hero.hint": "Drag to rotate · scroll to zoom · click a node",
    "feat.kicker": "Why Damon",
    "feat.title": "Agents stay theirs, control stays yours",
    "feat.lead": "If you already subscribe to an agent CLI, there is nothing to configure. Damon detects installed CLIs and inherits their logins and tools.",
    "f1t": "Zero-config backends",
    "f1d": "claude, codex, and omp on PATH register themselves. Any other agent plugs in via one <code>[backends.X]</code> block.",
    "f2t": "Permissions flow to your surface",
    "f2d": "The agent's permission ask is relayed to the web UI, CLI, or an <code>allow</code>/<code>deny</code> reply in Telegram, Discord, or Slack.",
    "f3t": "Searchable history",
    "f3d": "Every conversation lands in SQLite + FTS5. <code>damon search \"error timeout\"</code> full-text-searches all of it.",
    "f4t": "Chat channels built in",
    "f4d": "Telegram, Discord, and Slack adapters ship as separate binaries. Per-chat sessions, streamed replies.",
    "f5t": "Damon holds no secrets",
    "f5d": "Agents manage their own auth. The Damon config is a port and maybe a token.",
    "f6t": "Reachable from anywhere",
    "f6d": "Serve <code>wss</code> with your certs, or run <code>damon-relay</code> and dial out — X25519 + AES-256-GCM E2E encrypted.",
    "f7t": "Built to stay resident",
    "f7d": "The Rust daemon stays lean. Agent processes are the agents' business.",
    "arch.kicker": "Architecture",
    "arch.title": "One API, one daemon",
    "arch.lead": "Every client speaks the same JSON-RPC over WebSocket (<code>/ws</code>, protocol v2), and the daemon drives the agent CLIs as subprocesses.",
    "arch.chip.web": "Web UI", "arch.chip.desktop": "Desktop app", "arch.chip.code": "Your code",
    "arch.api": "One API — JSON-RPC over WebSocket · <code>/ws</code> · protocol v2",
    "arch.core.sub": "resident daemon · Rust",
    "arch.m1": "Backend registry — detect, spawn, restart CLIs",
    "arch.m2": "Session manager — lifecycle, permission relay, cancel",
    "arch.m3": "Session store — SQLite + FTS5",
    "arch.m4": "Channel bridges / E2E relay",
    "arch.agents.note": "Model, tools, subscription auth, and context are all agent-owned",
    "arch.proto": "Wire protocol docs →",
    "arch.integration": "Client integration guide →",
    "quick.kicker": "Quickstart",
    "quick.title": "Three commands and you're done",
    "quick.install": "Install", "quick.run": "Run", "quick.check": "Verify",
    "quick.c.npm": "npm (prebuilt binaries)",
    "quick.c.cargo": "crates.io",
    "quick.c.brew": "Homebrew tap",
    "quick.run.c1": "writes a starter config on first run",
    "quick.run.c2": "which backends are installed?",
    "quick.service": "Keep it resident",
    "quick.service.c1": "launchd / systemd user / Task Scheduler",
    "quick.ui.t": "Chat in the bundled web UI",
    "quick.ui.p": "Sessions, streaming, permission prompts — no install.",
    "chan.kicker": "Chat channels",
    "chan.title": "Your chat app is the control room",
    "chan.lead": "Each chat maps to its own agent session, replies stream, and tool-permission requests are approved with an <code>allow</code>/<code>deny</code> reply.",
    "remote.kicker": "Remote access",
    "remote.title": "From anywhere, safely",
    "remote.r1t": "Tailscale (recommended)",
    "remote.r1d": "Attach at <code>ws://&lt;tailscale-ip&gt;:9470/ws</code> — WireGuard E2E, no daemon changes.",
    "remote.r2t": "Direct TLS",
    "remote.r2d": "Set <code>tls_cert</code>/<code>tls_key</code> to serve <code>wss</code>. Non-loopback binds refuse to start without <code>auth_token</code>.",
    "remote.r3t": "Self-hosted relay",
    "remote.r3d": "Run <code>damon-relay</code> on a public host and the daemon dials out — no inbound port. X25519 key exchange → AES-256-GCM; the relay sees only ciphertext.",
    "eco.kicker": "Ecosystem",
    "eco.title": "One client per language",
    "eco.npm.d": "The dependency-free Node client bundled in the npm package.",
    "eco.py.d": "The official asyncio client.",
    "eco.py.link": "python/ directory →",
    "eco.rs.d": "The core crate on crates.io.",
    "foot.tag": "The name is a pun on daemon — and literally the architecture.",
    "foot.license": "Dual-licensed MIT or Apache-2.0",
    "copy.aria": "Copy command",
    "copy.done": "Copied!",
    "tip.click": "Click to jump to the section",
  },
};

let lang = "ko";
try {
  const saved = localStorage.getItem("damon-lang");
  if (saved === "ko" || saved === "en") lang = saved;
} catch { /* private mode */ }

function t(key) {
  return (I18N[lang] && I18N[lang][key]) ?? I18N.ko[key] ?? key;
}

let sceneApi = null; // set once the 3D scene is up

function applyLang() {
  document.documentElement.lang = lang;
  document.title = t("meta.title");
  for (const el of document.querySelectorAll("[data-i18n]")) {
    el.innerHTML = t(el.dataset.i18n);
  }
  for (const el of document.querySelectorAll("[data-i18n-aria]")) {
    el.setAttribute("aria-label", t(el.dataset.i18nAria));
  }
  const toggle = document.getElementById("lang-toggle");
  if (toggle) toggle.textContent = lang === "ko" ? "EN" : "한국어";
  const tip = document.getElementById("node-tip");
  if (tip) tip.dataset.clickHint = t("tip.click");
  sceneApi?.setLang(lang);
}

const toggle = document.getElementById("lang-toggle");
if (toggle) {
  toggle.addEventListener("click", () => {
    lang = lang === "ko" ? "en" : "ko";
    try { localStorage.setItem("damon-lang", lang); } catch { /* ignore */ }
    applyLang();
  });
}

/* --------------------------- copy buttons --------------------------- */

document.addEventListener("click", async (e) => {
  const btn = e.target.closest("[data-copy]");
  if (!btn) return;
  const text = btn.dataset.copy;
  let ok = false;
  try {
    await navigator.clipboard.writeText(text);
    ok = true;
  } catch {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.cssText = "position:fixed;opacity:0";
    document.body.appendChild(ta);
    ta.select();
    try { ok = document.execCommand("copy"); } catch { ok = false; }
    ta.remove();
  }
  if (ok) {
    btn.classList.add("copied");
    const prev = btn.getAttribute("aria-label");
    btn.setAttribute("aria-label", t("copy.done"));
    setTimeout(() => {
      btn.classList.remove("copied");
      btn.setAttribute("aria-label", prev || t("copy.aria"));
    }, 1400);
  }
});

/* ----------------------------- 3D scene ----------------------------- */

// Scene graph: Damon core at center, agent CLIs on the inner orbit,
// clients on the outer tilted orbit, JSON-RPC "packets" travelling the
// connections. Mirrors the architecture diagram one-to-one.

const NODES = {
  core: {
    name: { ko: "Damon core", en: "Damon core" },
    desc: {
      ko: "상주 데몬 · Rust — 세션·권한·히스토리·릴레이",
      en: "Resident daemon · Rust — sessions, permissions, history, relay",
    },
    color: "#5b8cff", target: "#architecture",
  },
  agents: [
    { name: { ko: "Claude Code", en: "Claude Code" },
      desc: { ko: "stream-json 백엔드", en: "stream-json backend" },
      color: "#d97757", target: "#features" },
    { name: { ko: "Codex CLI", en: "Codex CLI" },
      desc: { ko: "app-server 백엔드", en: "app-server backend" },
      color: "#10a37f", target: "#features" },
    { name: { ko: "Oh My Pi", en: "Oh My Pi" },
      desc: { ko: "RPC 모드 백엔드", en: "RPC-mode backend" },
      color: "#a78bfa", target: "#features" },
  ],
  clients: [
    { name: { ko: "CLI", en: "CLI" },
      desc: { ko: "가장 얇은 클라이언트", en: "The thinnest client" },
      color: "#7dd3fc", target: "#quickstart" },
    { name: { ko: "웹 UI", en: "Web UI" },
      desc: { ko: "번들 웹 UI — 설치 불필요", en: "Bundled web UI — no install" },
      color: "#7dd3fc", target: "#quickstart" },
    { name: { ko: "Telegram", en: "Telegram" },
      desc: { ko: "채팅이 곧 컨트롤 룸", en: "Chat is the control room" },
      color: "#26a5e4", target: "#channels" },
    { name: { ko: "Discord", en: "Discord" },
      desc: { ko: "채팅이 곧 컨트롤 룸", en: "Chat is the control room" },
      color: "#5865f2", target: "#channels" },
    { name: { ko: "Slack", en: "Slack" },
      desc: { ko: "채팅이 곧 컨트롤 룸", en: "Chat is the control room" },
      color: "#e0507e", target: "#channels" },
    { name: { ko: "데스크톱 앱", en: "Desktop app" },
      desc: { ko: "같은 데몬에 붙는 얇은 클라이언트", en: "A thin client on the same daemon" },
      color: "#7dd3fc", target: "#quickstart" },
  ],
};

const AGENT_R = 5.3;
const CLIENT_R = 8.4;

function roundRectPath(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function makeLabelSprite(THREE, text, accent) {
  const s = 2; // supersample for crisp text
  const font = `600 ${13 * s}px -apple-system, BlinkMacSystemFont, "Segoe UI", "Apple SD Gothic Neo", "Noto Sans KR", sans-serif`;
  const c = document.createElement("canvas");
  let ctx = c.getContext("2d");
  ctx.font = font;
  const tw = Math.ceil(ctx.measureText(text).width);
  c.width = tw + 30 * s;
  c.height = 24 * s;
  ctx = c.getContext("2d");
  ctx.font = font;
  ctx.clearRect(0, 0, c.width, c.height);
  ctx.fillStyle = "rgba(9, 13, 22, 0.88)";
  ctx.strokeStyle = accent;
  ctx.lineWidth = 1.2 * s;
  roundRectPath(ctx, 1 * s, 1 * s, c.width - 2 * s, c.height - 2 * s, 11 * s);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = "#e6ebf5";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(text, c.width / 2, c.height / 2 + s);
  const tex = new THREE.CanvasTexture(c);
  tex.colorSpace = THREE.SRGBColorSpace;
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({
    map: tex, transparent: true, depthTest: false,
  }));
  sprite.renderOrder = 20;
  const baseH = 0.62;
  sprite.scale.set(baseH * c.width / c.height, baseH, 1);
  return sprite;
}

function makeGlowTexture(THREE) {
  const c = document.createElement("canvas");
  c.width = c.height = 128;
  const ctx = c.getContext("2d");
  const g = ctx.createRadialGradient(64, 64, 0, 64, 64, 64);
  g.addColorStop(0, "rgba(255,255,255,1)");
  g.addColorStop(0.25, "rgba(255,255,255,0.5)");
  g.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = g;
  ctx.fillRect(0, 0, 128, 128);
  const tex = new THREE.CanvasTexture(c);
  tex.colorSpace = THREE.SRGBColorSpace;
  return tex;
}

async function initScene() {
  const container = document.getElementById("scene");
  const hero = container?.closest(".hero");
  if (!container || !hero) return;

  let THREE, OrbitControls;
  try {
    THREE = await import("three");
    ({ OrbitControls } = await import("three/addons/controls/OrbitControls.js"));
  } catch {
    hero.classList.add("scene-fallback");
    return;
  }

  let renderer;
  try {
    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    if (!renderer.getContext()) throw new Error("no webgl context");
  } catch {
    hero.classList.add("scene-fallback");
    return;
  }

  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
  const motion = reduced ? 0 : 1;

  renderer.setPixelRatio(Math.min(devicePixelRatio || 1, 2));
  renderer.setSize(container.clientWidth, container.clientHeight);
  container.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  scene.fog = new THREE.Fog(0x0a0e16, 20, 52);

  const camera = new THREE.PerspectiveCamera(
    42, container.clientWidth / container.clientHeight, 0.1, 120);
  camera.position.set(6.5, 5, 17.5);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.target.set(0, 0.4, 0);
  controls.enableDamping = true;
  controls.dampingFactor = 0.06;
  controls.enablePan = false;
  controls.minDistance = 9;
  controls.maxDistance = 32;
  controls.maxPolarAngle = Math.PI * 0.72;
  controls.autoRotate = !reduced;
  controls.autoRotateSpeed = 0.55;

  scene.add(new THREE.AmbientLight(0x8ea6ff, 0.7));
  const dir = new THREE.DirectionalLight(0x7dd3fc, 1.6);
  dir.position.set(6, 10, 4);
  scene.add(dir);
  const coreLight = new THREE.PointLight(0x5b8cff, 90, 40, 1.8);
  coreLight.position.set(0, 2.5, 0);
  scene.add(coreLight);

  /* ----- core ----- */
  const coreGroup = new THREE.Group();
  coreGroup.position.set(0, 0.4, 0);
  scene.add(coreGroup);

  const coreMat = new THREE.MeshStandardMaterial({
    color: 0x233046, emissive: 0x5b8cff, emissiveIntensity: 0.55,
    metalness: 0.65, roughness: 0.3, flatShading: true,
  });
  const coreMesh = new THREE.Mesh(new THREE.IcosahedronGeometry(1.9, 1), coreMat);
  coreMesh.userData.node = NODES.core;
  coreGroup.add(coreMesh);

  const wire = new THREE.LineSegments(
    new THREE.WireframeGeometry(new THREE.IcosahedronGeometry(2.08, 1)),
    new THREE.LineBasicMaterial({ color: 0x7dd3fc, transparent: true, opacity: 0.3 }));
  coreGroup.add(wire);

  const glowTex = makeGlowTexture(THREE);
  const coreGlow = new THREE.Sprite(new THREE.SpriteMaterial({
    map: glowTex, color: 0x5b8cff, transparent: true, opacity: 0.5,
    blending: THREE.AdditiveBlending, depthWrite: false,
  }));
  coreGlow.scale.setScalar(9);
  coreGroup.add(coreGlow);

  const coreLabel = makeLabelSprite(THREE, NODES.core.name[lang], NODES.core.color);
  coreLabel.position.set(0, 3.1, 0);
  coreGroup.add(coreLabel);
  NODES.core.label = coreLabel;

  /* ----- orbits ----- */
  const agentPivot = new THREE.Group();
  scene.add(agentPivot);
  const clientPivot = new THREE.Group();
  clientPivot.rotation.set(0.30, 0, -0.14);
  scene.add(clientPivot);

  const ringMat = (opacity) =>
    new THREE.MeshBasicMaterial({ color: 0x5b8cff, transparent: true, opacity });
  const ringA = new THREE.Mesh(new THREE.TorusGeometry(AGENT_R, 0.014, 8, 180), ringMat(0.35));
  ringA.rotation.x = Math.PI / 2;
  agentPivot.add(ringA);
  const ringB = new THREE.Mesh(new THREE.TorusGeometry(CLIENT_R, 0.011, 8, 200), ringMat(0.22));
  ringB.rotation.x = Math.PI / 2;
  clientPivot.add(ringB);

  /* ----- nodes ----- */
  const pickables = [coreMesh];

  function addNode(def, parent, geom, baseScale) {
    const color = new THREE.Color(def.color);
    const mesh = new THREE.Mesh(geom, new THREE.MeshStandardMaterial({
      color, emissive: color, emissiveIntensity: 0.35,
      metalness: 0.3, roughness: 0.45, flatShading: true,
    }));
    mesh.userData.node = def;
    mesh.userData.baseScale = baseScale;
    const halo = new THREE.Sprite(new THREE.SpriteMaterial({
      map: glowTex, color, transparent: true, opacity: 0.35,
      blending: THREE.AdditiveBlending, depthWrite: false,
    }));
    halo.scale.setScalar(2.1 * baseScale);
    mesh.add(halo);
    def.halo = halo;
    const label = makeLabelSprite(THREE, def.name[lang], def.color);
    label.position.y = 1.15 * baseScale;
    mesh.add(label);
    def.label = label;
    def.mesh = mesh;
    parent.add(mesh);
    pickables.push(mesh);
    return mesh;
  }

  NODES.agents.forEach((def) =>
    addNode(def, agentPivot, new THREE.SphereGeometry(0.5, 24, 24), 1));
  NODES.clients.forEach((def) =>
    addNode(def, clientPivot, new THREE.IcosahedronGeometry(0.36, 0), 0.72));

  /* ----- connections + packets ----- */
  const connections = [];
  const tmpA = new THREE.Vector3();
  const tmpB = new THREE.Vector3();

  function addConnection(def, fromCore) {
    const color = new THREE.Color(def.color);
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(new Float32Array(6), 3));
    const line = new THREE.Line(geo, new THREE.LineBasicMaterial({
      color, transparent: true, opacity: 0.12, blending: THREE.AdditiveBlending,
    }));
    scene.add(line);
    const packetMat = new THREE.MeshBasicMaterial({
      color: 0xcfe0ff, transparent: true, opacity: 0.9,
      blending: THREE.AdditiveBlending, depthWrite: false,
    });
    const packet = new THREE.Mesh(new THREE.SphereGeometry(0.07, 8, 8), packetMat);
    scene.add(packet);
    connections.push({ def, line, packet, fromCore,
      speed: 0.16 + Math.random() * 0.14, phase: Math.random() });
  }

  NODES.agents.forEach((d) => addConnection(d, true));
  NODES.clients.forEach((d) => addConnection(d, false));

  /* ----- starfield ----- */
  const starCount = 500;
  const starPos = new Float32Array(starCount * 3);
  for (let i = 0; i < starCount; i++) {
    const r = 26 + Math.random() * 22;
    const th = Math.random() * Math.PI * 2;
    const ph = Math.acos(2 * Math.random() - 1);
    starPos[i * 3] = r * Math.sin(ph) * Math.cos(th);
    starPos[i * 3 + 1] = r * Math.cos(ph);
    starPos[i * 3 + 2] = r * Math.sin(ph) * Math.sin(th);
  }
  const starGeo = new THREE.BufferGeometry();
  starGeo.setAttribute("position", new THREE.BufferAttribute(starPos, 3));
  const stars = new THREE.Points(starGeo, new THREE.PointsMaterial({
    color: 0x9db7e8, size: 0.07, transparent: true, opacity: 0.75,
  }));
  scene.add(stars);

  /* ----- interaction ----- */
  const raycaster = new THREE.Raycaster();
  const pointer = new THREE.Vector2();
  const tip = document.getElementById("node-tip");
  let hovered = null;
  let downX = 0, downY = 0;

  function pick(e) {
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
    pointer.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(pointer, camera);
    const hits = raycaster.intersectObjects(pickables, false);
    return hits.length ? hits[0].object.userData.node : null;
  }

  function showTip(node, e) {
    if (!tip || !node) return;
    const rect = renderer.domElement.getBoundingClientRect();
    (node === NODES.core ? tmpA : node.mesh.getWorldPosition(tmpA));
    tmpB.copy(tmpA);
    tmpB.y += node === NODES.core ? 2.2 : 0.9;
    tmpB.project(camera);
    const x = (tmpB.x * 0.5 + 0.5) * rect.width;
    const y = (-tmpB.y * 0.5 + 0.5) * rect.height;
    tip.hidden = false;
    tip.innerHTML = `<strong style="color:${node.color}"></strong><span></span>`;
    tip.querySelector("strong").textContent = node.name[lang];
    tip.querySelector("span").textContent = node.desc[lang];
    tip.style.left = Math.max(10, Math.min(rect.width - 10, x)) + "px";
    tip.style.top = Math.max(10, y) + "px";
  }

  renderer.domElement.addEventListener("pointermove", (e) => {
    hovered = pick(e);
    renderer.domElement.style.cursor = hovered ? "pointer" : "grab";
    if (hovered) showTip(hovered, e);
    else if (tip) tip.hidden = true;
  });
  renderer.domElement.addEventListener("pointerleave", () => {
    hovered = null;
    if (tip) tip.hidden = true;
  });
  renderer.domElement.addEventListener("pointerdown", (e) => {
    downX = e.clientX; downY = e.clientY;
  });
  renderer.domElement.addEventListener("pointerup", (e) => {
    if (Math.hypot(e.clientX - downX, e.clientY - downY) > 6) return;
    const node = pick(e);
    if (node?.target) {
      document.querySelector(node.target)
        ?.scrollIntoView({ behavior: reduced ? "auto" : "smooth" });
      if (tip) tip.hidden = true;
    }
  });

  /* ----- resize / visibility ----- */
  new ResizeObserver(() => {
    const w = container.clientWidth, h = container.clientHeight;
    if (!w || !h) return;
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    renderer.setSize(w, h);
  }).observe(container);

  /* ----- loop ----- */
  const clock = new THREE.Clock();
  let raf = 0;
  let running = true;

  function frame() {
    if (!running) return;
    raf = requestAnimationFrame(frame);
    const dt = Math.min(clock.getDelta(), 0.05);
    const time = clock.elapsedTime;

    NODES.agents.forEach((n, i) => {
      const a = time * 0.14 * motion + (i * Math.PI * 2) / 3;
      n.mesh.position.set(Math.cos(a) * AGENT_R, 0, Math.sin(a) * AGENT_R);
    });
    NODES.clients.forEach((n, i) => {
      const a = -time * 0.09 * motion + (i * Math.PI * 2) / 6;
      n.mesh.position.set(Math.cos(a) * CLIENT_R, 0, Math.sin(a) * CLIENT_R);
    });

    coreMesh.rotation.y = time * 0.25 * motion;
    coreMesh.rotation.x = time * 0.1 * motion;
    wire.rotation.copy(coreMesh.rotation);
    const pulse = motion * Math.sin(time * 1.6);
    coreMesh.scale.setScalar(1 + 0.03 * pulse);
    coreMat.emissiveIntensity = 0.55 + 0.18 * pulse;
    coreGlow.material.opacity = 0.5 + 0.12 * pulse;

    coreGroup.getWorldPosition(tmpA);
    for (const c of connections) {
      c.def.mesh.getWorldPosition(tmpB);
      const attr = c.line.geometry.getAttribute("position");
      attr.setXYZ(0, tmpA.x, tmpA.y, tmpA.z);
      attr.setXYZ(1, tmpB.x, tmpB.y, tmpB.z);
      attr.needsUpdate = true;
      c.line.material.opacity = 0.10 + 0.05 * Math.sin(time * 1.2 + c.phase * 9);

      const tt = motion ? (time * c.speed + c.phase) % 1 : 0.5;
      const p = c.fromCore
        ? tmpA.clone().lerp(tmpB, tt)
        : tmpB.clone().lerp(tmpA, tt);
      c.packet.position.copy(p);
      c.packet.material.opacity = Math.sin(Math.PI * tt) * 0.9 * (motion ? 1 : 0.4);
    }

    for (const node of [...NODES.agents, ...NODES.clients]) {
      const target = (hovered === node ? 1.22 : 1) * node.mesh.userData.baseScale;
      node.mesh.scale.lerp(new THREE.Vector3(target, target, target), 0.15);
      node.halo.material.opacity = hovered === node ? 0.6 : 0.35;
    }
    const coreTarget = hovered === NODES.core ? 1.15 : 1;
    coreGroup.scale.lerp(new THREE.Vector3(coreTarget, coreTarget, coreTarget), 0.12);

    stars.rotation.y += dt * 0.005 * motion;

    controls.update();
    renderer.render(scene, camera);
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      running = false;
      cancelAnimationFrame(raf);
    } else if (!running) {
      running = true;
      clock.getDelta(); // discard the pause
      frame();
    }
  });

  frame();

  /* ----- language hook ----- */
  sceneApi = {
    setLang(l) {
      const defs = [NODES.core, ...NODES.agents, ...NODES.clients];
      for (const d of defs) {
        if (!d.label) continue;
d.label.material.map?.dispose();
const fresh = makeLabelSprite(THREE, d.name[l], d.color);
d.label.material.map = fresh.material.map;
d.label.material.needsUpdate = true;
d.label.scale.copy(fresh.scale);
fresh.material.map = null;
fresh.material.dispose();
      }
      if (hovered && tip && !tip.hidden) {
        tip.querySelector("strong").textContent = hovered.name[l];
        tip.querySelector("span").textContent = hovered.desc[l];
      }
    },
  };
}

applyLang();
initScene();
