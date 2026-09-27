// Damon intro site — i18n, copy buttons, 3D architecture scene, live demos.

/* ------------------------------ i18n ------------------------------ */

const I18N = {
  ko: {
    "meta.title": "Damon — 데몬 하나. 모든 코딩 에이전트.",
    "nav.new": "신규", "nav.features": "기능", "nav.arch": "구조", "nav.quick": "시작하기",
    "nav.tools": "도구", "nav.chan": "채널", "nav.any": "어디서든", "nav.eco": "생태계",
    "hero.badge": "Rust 기반 로컬 에이전트 컨트롤 데몬",
    "hero.title1": "데몬 하나.",
    "hero.title2": "모든 코딩 에이전트.",
    "hero.lead": "Damon은 아홉 코딩 에이전트 CLI — Claude Code·Codex CLI·Oh My Pi·Pi·Qwen Code·Droid·OpenCode·MiMo·ZCode — 를 각자의 네이티브 양방향 프로토콜로 구동하는 Rust 상주 데몬입니다. 모델·툴·인증은 에이전트 몫 — Damon은 세션, 권한 릴레이, 검색 가능한 히스토리, 스킬 허브, PR 워크트리, 채팅 채널, 어디서든 접근을 소유합니다.",
    "hero.cta.new": "새 기능 보기",
    "hero.cta.quick": "퀵스타트",
    "hero.cta.gh": "GitHub에서 보기",
    "hero.hint": "드래그로 회전 · 스크롤로 확대 · 노드 클릭",
    "strip.title": "PATH에 있으면 자동 등록 — 네이티브 양방향 백엔드",
    "strip.more": "권한 릴레이·질문·인터럽트가 온전히 릴레이되는 프로토콜만 카탈로그에 등재됩니다",
    "new.kicker": "방금 main에 착지",
    "new.title": "지금 막 합류한 것들",
    "new.lead": "네 가지가 한 번에 왔습니다 — 아래 카드는 실제 동작을 축소해서 재현한 라이브 데모입니다.",
    "wt.card.t": "PR 하나, 격리된 워크트리 하나",
    "wt.card.d": "PR 번호만 주면 refs/pull/N/head를 격리된 git worktree로 받아 프로젝트로 등록합니다. 단계별 진행 프레임, 진행 중 취소, gh 프리뷰 — 실패해도 메인 체크아웃은 청정합니다.",
    "wt.s1": "PR → owner/repo 확인",
    "wt.s3": "git worktree add",
    "wt.s4": "kind:\"worktree\" 프로젝트 등록",
    "wt.result": "여기서 세션 시작 → session.create {projectId}",
    "wt.cancelled": "취소 — 진행 중인 git 프로세스 그룹을 kill 했습니다",
    "sk.card.t": "스킬 한 번 설치, 모든 CLI에 동기화",
    "sk.card.d": "GitHub·skills.sh에서 스킬 패키지를 찾아 설치하면, 허브가 설치된 모든 CLI의 네이티브 스킬 디렉터리로 심링크합니다. CLI는 여전히 자기 스킬을 스스로 로드합니다.",
    "sk.hub": "skills-hub 스토어",
    "sk.note": "설치 → 동기화 → catalog.commands가 별도 설정 없이 스킬을 인지",
    "rv.card.t": "배치 덤프 없는 스트리밍 리빌",
    "rv.card.d": "도착 박자에 맞춰 글자가 흐릅니다. 커서는 문서 꼬리에 붙어 재파싱에도 흐름이 안 끊기고, 코드펜스는 닫히는 순간 점등됩니다. 그래핌 안전 — CJK·이모지도 뜯기지 않습니다.",
    "rv.replay": "재생",
    "rv.s1": "**페이싱된 리빌**은 배치 덤프를 없앱니다 — 도착 박자에 맞춰 글자가 흐릅니다.",
    "rv.s2": "커서는 문서 꼬리에 붙어 — 재파싱이 리빌을 리셋하지 않습니다. 펜스가 닫히면 코드가 점등됩니다.",
    "cp.card.t": "입력줄이 곧 런처 — / · @ · !",
    "cp.card.d": "<code>/</code>는 CLI가 이미 아는 커맨드·스킬 카탈로그, <code>@</code>는 ripgrep 워커가 걸어 만든 파일 인덱스, <code>!</code>는 프롬프트 라이브러리. 프롬프트 히스토리는 데몬이 저장 — 폰에서도 데스크톱 완성이 보입니다. 직접 입력해 보세요.",
    "cp.ph": "/ 커맨드 · @ 파일 · ! 프롬프트",
    "cp.msg": "이 PR 리뷰해줘",
    "cp.shadow": "workspace가 같은 이름의 글로벌 항목을 가립니다",
    "cp.hint": "를 누르거나 직접 타이핑 — ▶ 버튼은 자동 재생",
    "cp.empty": "일치하는 항목 없음",
    "cp.sent": "프롬프트 본문 삽입됨",
    "feat.kicker": "왜 Damon인가",
    "feat.title": "에이전트는 그대로, 제어는 한 곳에서",
    "feat.lead": "이미 에이전트 CLI를 구독 중이라면 설정할 게 하나도 없습니다. Damon은 설치된 CLI를 감지해 로그인과 툴을 그대로 물려받습니다.",
    "f1t": "설정 없는 백엔드",
    "f1d": "PATH의 claude·codex·omp·pi·qwen·droid·opencode·mimo·zcode면 자동 등록됩니다. 카탈로그 백엔드는 전부 네이티브 양방향 프로토콜 — 스트리밍·권한 릴레이·인터럽트 — 을 말합니다.",
    "f2t": "권한은 당신 채널로",
    "f2d": "에이전트의 권한 요청을 웹 UI·CLI·텔레그램/디스코드/슬랙의 <code>allow</code>/<code>deny</code> 답장으로 릴레이합니다.",
    "f3t": "검색 가능한 히스토리",
    "f3d": "모든 대화가 SQLite + FTS5에 기록됩니다. <code>damon search \"error timeout\"</code>으로 전체 이력 전문 검색.",
    "f4t": "채팅 채널 내장",
    "f4d": "Telegram·Discord·Slack 어댑터가 별도 바이너리로 제공됩니다. 채팅별 세션 자동 매핑, 스트리밍 응답.",
    "f5t": "시크릿은 Damon에 없다",
    "f5d": "에이전트가 자기 인증을 관리합니다. Damon config는 포트와 토큰 정도뿐.",
    "f6t": "어디서든 접근",
    "f6d": "<code>wss</code> 직접 서빙 또는 <code>damon-relay</code> 아웃바운드 터널 — X25519 + AES-256-GCM E2E 암호화. 릴레이는 웹 UI도 서빙합니다.",
    "f7t": "상주하도록 설계",
    "f7d": "Rust 데몬 본체는 가볍게 유지됩니다. 에이전트 프로세스는 에이전트의 몫입니다.",
    "f8t": "프롬프트도 버전 관리",
    "f8d": "재사용 프롬프트를 마크다운 + 가벼운 프론트매터로 저장. <code>prompts.*</code> CRUD와 스코프 이동, 컴포저 <code>!</code> 피커에 그대로 노출됩니다.",
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
    "arch.m5": "스킬 허브 · 프롬프트 라이브러리",
    "arch.m6": "워크트리 워크스페이스 — PR 격리 체크아웃",
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
    "quick.ui.p": "세션, 스트리밍, 권한 프롬프트, /·@·! 피커까지 — 설치 불필요. <code>?revealDemo=1</code>로 리빌 파이프라인 데모.",
    "chan.kicker": "채팅 채널",
    "chan.title": "채팅 앱이 곧 컨트롤 룸",
    "chan.lead": "각 채팅이 고유한 에이전트 세션에 매핑되고 응답은 스트리밍됩니다. tool 권한 요청은 네이티브 버튼이나 <code>allow</code>/<code>deny</code> 답장으로 승인합니다. 파일은 프롬프트 첨부로 ride along.",
    "ch.c1": "새 세션", "ch.c2": "분기", "ch.c3": "턴 취소", "ch.c4": "프로젝트 변경",
    "ch.c5": "백엔드 변경", "ch.c6": "토큰 사용량", "ch.c7": "전체 세션", "ch.c8": "이어잡기", "ch.c9": "완료·권한 알림",
    "any.kicker": "어디서든",
    "any.title": "인터넷만 되면, 어디서든",
    "any.lead": "책상에서 시작한 작업은 집을 나선 뒤에도 계속됩니다. 폰의 채팅과 브라우저가 같은 데몬의 리모컨이 됩니다 — 모든 통신은 종단간 암호화.",
    "any.f1t": "아침 · 책상",
    "any.f1d": "웹 UI에서 큰 작업을 시작",
    "any.f2t": "외출 · 화면 꺼짐",
    "any.f2d": "턴은 데몬에서 계속 — 끊기지 않습니다",
    "any.f3t": "이동 중 · 폰",
    "any.f3d": "<code>!sessions</code> → <code>!resume</code>로 이어잡기",
    "any.f4t": "완료 · 알림",
    "any.f4d": "끝나면 채팅으로 소식 — 권한도 답장으로 승인",
    "any.c1t": "화면이 꺼져도 작업은 계속",
    "any.c1d": "<code>turn.start {detach: true}</code> — 연결이 끊겨도 턴이 끝까지 돌고 결과는 히스토리에 남습니다. 다시 접속하면 이어서 보입니다.",
    "any.c2t": "채팅에서 이어잡기 + 알림",
    "any.c2d": "<code>!sessions</code>·<code>!resume</code>·<code>!watch</code> — 어느 화면에서 돌든 세션이 끝나거나 권한을 요청하면 채팅으로 알리고, 답장으로 승인합니다.",
    "any.c3t": "릴레이가 웹 UI를 서빙",
    "any.c3d": "브라우저로 릴레이 호스트를 열고 데몬 이름+토큰 입력 — 페이지가 직접 E2E 핸드셰이크합니다. VPN·포트포워딩·호스팅 불필요.",
    "any.t1": "Tailscale",
    "any.t2": "직접 TLS · wss",
    "any.t3": "자체 릴레이 — 다이얼아웃 · X25519 + AES-256-GCM · 브라우저 클라이언트 내장",
    "eco.kicker": "생태계",
    "eco.title": "모든 언어에서 한 줄로",
    "eco.npm.d": "npm 패키지에 포함된 무의존성 클라이언트.",
    "eco.py.d": "asyncio 기반 공식 클라이언트.",
    "eco.py.link": "python/ 디렉터리 →",
    "eco.rs.d": "crates.io의 코어 크레이트.",
    "tools.kicker": "세션 도구상자",
    "tools.title": "히스토리는 자산입니다",
    "tools.lead": "모든 대화가 SQLite에 쌓입니다 — 검색하고, 분기하고, 내보내고, 백업하세요. 데몬이 살아 있는 동안에도 안전합니다.",
    "tools.t1": "대화", "tools.t2": "탐색", "tools.t3": "보존", "tools.t4": "신규 CLI 그룹",
    "tools.c1": "이전 세션 이어서",
    "tools.c2": "데몬 밖에서 만든 세션 흡수",
    "tools.c3": "토큰 사용량",
    "tools.c4": "히스토리 복사해 분기",
    "tools.c5": "Markdown 트랜스크립트",
    "tools.c6": "실행 중에도 안전한 스냅샷",
    "foot.tag": "이름은 daemon의 말장난이자, 말 그대로 실제 아키텍처입니다.",
    "foot.license": "듀얼 라이선스 MIT 또는 Apache-2.0",
    "copy.aria": "명령 복사",
    "copy.done": "복사됨!",
    "tip.click": "클릭하면 해당 섹션으로 이동",
  },
  en: {
    "meta.title": "Damon — One daemon. Every coding agent.",
    "nav.new": "New", "nav.features": "Features", "nav.arch": "Architecture", "nav.quick": "Quickstart",
    "nav.tools": "Toolbox", "nav.chan": "Channels", "nav.any": "Anywhere", "nav.eco": "Ecosystem",
    "hero.badge": "Local agent-control daemon in Rust",
    "hero.title1": "One daemon.",
    "hero.title2": "Every coding agent.",
    "hero.lead": "Damon is a resident Rust daemon that drives nine coding-agent CLIs — Claude Code, Codex CLI, Oh My Pi, Pi, Qwen Code, Droid, OpenCode, MiMo, ZCode — over their native bidirectional protocols. Model, tools, and auth belong to the agents — Damon owns sessions, permission relaying, searchable history, the skills hub, PR worktrees, chat channels, and from-anywhere access.",
    "hero.cta.new": "See what's new",
    "hero.cta.quick": "Quickstart",
    "hero.cta.gh": "View on GitHub",
    "hero.hint": "Drag to rotate · scroll to zoom · click a node",
    "strip.title": "On PATH, self-registered — native bidirectional backends",
    "strip.more": "Only protocols whose permission relay, questions, and interrupt fully round-trip make the catalog",
    "new.kicker": "just landed on main",
    "new.title": "Fresh off the main branch",
    "new.lead": "Four things at once — every card below is a live demo replaying the real behavior in miniature.",
    "wt.card.t": "One PR, one isolated worktree",
    "wt.card.d": "Give it a PR number and it fetches refs/pull/N/head into an isolated git worktree and registers it as a project. Per-stage progress frames, mid-fetch cancellation, gh previews — and your main checkout stays clean whatever happens.",
    "wt.s1": "resolve PR → owner/repo",
    "wt.s3": "git worktree add",
    "wt.s4": "register kind:\"worktree\" project",
    "wt.result": "start a session here → session.create {projectId}",
    "wt.cancelled": "cancelled — the in-flight git process group was killed",
    "sk.card.t": "Install a skill once, sync it everywhere",
    "sk.card.d": "Discover skill packages on GitHub and skills.sh; the hub symlinks them into every installed CLI's native skills directory. The CLIs keep loading skills themselves.",
    "sk.hub": "skills-hub store",
    "sk.note": "install → sync → catalog.commands picks skills up with no extra wiring",
    "rv.card.t": "Streaming reveal without batch dumps",
    "rv.card.d": "Text flows out over the arrival cadence. A tail-anchored cursor survives re-parsing, and code fences light up the moment their closing marker lands. Grapheme-safe — CJK and emoji don't tear.",
    "rv.replay": "Replay",
    "rv.s1": "**Paced reveal** kills the batch dump — characters flow out over the arrival cadence.",
    "rv.s2": "The cursor rides the document tail — re-parsing never resets the reveal. Fences light up when they close.",
    "cp.card.t": "The input line is a launcher — / · @ · !",
    "cp.card.d": "<code>/</code> opens the command and skill catalog the CLIs already know, <code>@</code> a file index walked by a ripgrep worker, <code>!</code> the prompt library. Prompt history lives in the daemon — your phone sees your desktop's completions. Type into it.",
    "cp.ph": "/ commands · @ files · ! prompts",
    "cp.msg": "review this PR for me",
    "cp.shadow": "workspace shadows same-named global entries",
    "cp.hint": "or just type — ▶ plays the scripted tour",
    "cp.empty": "no matches",
    "cp.sent": "prompt body inserted",
    "feat.kicker": "Why Damon",
    "feat.title": "Agents stay theirs, control stays yours",
    "feat.lead": "If you already subscribe to an agent CLI, there is nothing to configure. Damon detects installed CLIs and inherits their logins and tools.",
    "f1t": "Zero-config backends",
    "f1d": "claude, codex, omp, pi, qwen, droid, opencode, mimo, and zcode on PATH register themselves. Every catalog backend speaks a native bidirectional protocol — streaming, permission relay, interrupt.",
    "f2t": "Permissions flow to your surface",
    "f2d": "The agent's permission ask is relayed to the web UI, CLI, or an <code>allow</code>/<code>deny</code> reply in Telegram, Discord, or Slack.",
    "f3t": "Searchable history",
    "f3d": "Every conversation lands in SQLite + FTS5. <code>damon search \"error timeout\"</code> full-text-searches all of it.",
    "f4t": "Chat channels built in",
    "f4d": "Telegram, Discord, and Slack adapters ship as separate binaries. Per-chat sessions, streamed replies.",
    "f5t": "Damon holds no secrets",
    "f5d": "Agents manage their own auth. The Damon config is a port and maybe a token.",
    "f6t": "Reachable from anywhere",
    "f6d": "Serve <code>wss</code> with your certs, or run <code>damon-relay</code> and dial out — X25519 + AES-256-GCM E2E encrypted. The relay serves the web UI too.",
    "f7t": "Built to stay resident",
    "f7d": "The Rust daemon stays lean. Agent processes are the agents' business.",
    "f8t": "Prompts under version control",
    "f8d": "Reusable prompts live as markdown with light frontmatter. <code>prompts.*</code> CRUD and scope moves, exposed straight in the composer's <code>!</code> picker.",
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
    "arch.m5": "Skills hub · prompt library",
    "arch.m6": "Worktree workspaces — isolated PR checkouts",
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
    "quick.ui.p": "Sessions, streaming, permission prompts, /·@·! pickers — no install. <code>?revealDemo=1</code> demos the reveal pipeline.",
    "chan.kicker": "Chat channels",
    "chan.title": "Your chat app is the control room",
    "chan.lead": "Each chat maps to its own agent session, replies stream, and tool-permission requests are approved with native buttons or an <code>allow</code>/<code>deny</code> reply. Files ride along as prompt attachments.",
    "ch.c1": "new session", "ch.c2": "fork", "ch.c3": "cancel turn", "ch.c4": "change project",
    "ch.c5": "change backend", "ch.c6": "token usage", "ch.c7": "all sessions", "ch.c8": "pick up", "ch.c9": "finish/permission pings",
    "any.kicker": "Anywhere",
    "any.title": "Anywhere there's internet",
    "any.lead": "Work started at your desk keeps going after you leave. Your phone's chat and browser become remotes for the same daemon — every message end-to-end encrypted.",
    "any.f1t": "Morning · desk",
    "any.f1d": "kick off a big task in the web UI",
    "any.f2t": "Out · screen off",
    "any.f2d": "the turn keeps running on the daemon",
    "any.f3t": "Commute · phone",
    "any.f3d": "<code>!sessions</code> → <code>!resume</code> to pick it up",
    "any.f4t": "Done · notified",
    "any.f4d": "the chat hears when it finishes — approve asks by reply",
    "any.c1t": "Turns outlive the screen",
    "any.c1d": "<code>turn.start {detach: true}</code> — a dropped connection never cancels the turn; the outcome lands in history and greets you on reconnect.",
    "any.c2t": "Pick up + notifications in chat",
    "any.c2d": "<code>!sessions</code>·<code>!resume</code>·<code>!watch</code> — whichever surface runs the session, the chat is pinged on finish or permission asks, and answers by reply.",
    "any.c3t": "The relay serves the web UI",
    "any.c3d": "Open the relay host in a browser, enter daemon name + token — the page runs the E2E handshake itself. No VPN, no port forwarding, nothing to host.",
    "any.t1": "Tailscale",
    "any.t2": "Direct TLS · wss",
    "any.t3": "Self-hosted relay — dial-out · X25519 + AES-256-GCM · browser client built in",
    "eco.kicker": "Ecosystem",
    "eco.title": "One client per language",
    "eco.npm.d": "The dependency-free Node client bundled in the npm package.",
    "eco.py.d": "The official asyncio client.",
    "eco.py.link": "python/ directory →",
    "eco.rs.d": "The core crate on crates.io.",
    "tools.kicker": "Session toolbox",
    "tools.title": "History is an asset",
    "tools.lead": "Every conversation lands in SQLite — search it, fork it, export it, back it up. Safe even while the daemon runs.",
    "tools.t1": "Chat", "tools.t2": "Mine", "tools.t3": "Keep", "tools.t4": "New CLI groups",
    "tools.c1": "pick up an old session",
    "tools.c2": "adopt sessions made outside damon",
    "tools.c3": "token usage",
    "tools.c4": "branch from a copy",
    "tools.c5": "Markdown transcript",
    "tools.c6": "safe while damond runs",
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
  for (const el of document.querySelectorAll("[data-i18n-ph]")) {
    el.setAttribute("placeholder", t(el.dataset.i18nPh));
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

/* --------------------------- install tabs --------------------------- */

const installBox = document.getElementById("install-box");
if (installBox) {
  const code = installBox.querySelector("#install-cmd");
  const copyBtn = installBox.querySelector(".copy-btn");
  installBox.querySelectorAll(".itab").forEach((tab) => {
    tab.addEventListener("click", () => {
      installBox.querySelectorAll(".itab").forEach((b) => b.classList.remove("active"));
      tab.classList.add("active");
      code.textContent = tab.dataset.cmd;
      copyBtn.dataset.copy = tab.dataset.cmd;
    });
  });
}

/* --------------------------- scroll progress --------------------------- */

const progressBar = document.getElementById("scroll-progress");
function updateProgress() {
  if (!progressBar) return;
  const max = document.documentElement.scrollHeight - innerHeight;
  progressBar.style.transform = `scaleX(${max > 0 ? Math.min(scrollY / max, 1) : 0})`;
}
addEventListener("scroll", updateProgress, { passive: true });
addEventListener("resize", updateProgress, { passive: true });
updateProgress();

/* --------------------------- marquee --------------------------- */

const marqueeTrack = document.getElementById("marquee-track");
if (marqueeTrack && !matchMedia("(prefers-reduced-motion: reduce)").matches) {
  for (const child of [...marqueeTrack.children]) {
    const clone = child.cloneNode(true);
    clone.setAttribute("aria-hidden", "true");
    marqueeTrack.appendChild(clone);
  }
}

/* --------------------------- scroll reveal --------------------------- */

const REVEAL_SELS =
  ".section .card, .bento-card, .term, .ui-note, .arch-api, .arch-core, .arch-row, .docs-links, .flow-step, .transport-strip, .cchip";
const revealOK = "IntersectionObserver" in window
  && !matchMedia("(prefers-reduced-motion: reduce)").matches;
if (revealOK) {
  const els = [...document.querySelectorAll(REVEAL_SELS)];
  els.forEach((el) => {
    el.classList.add("reveal");
    const sibs = [...el.parentElement.children].filter((c) => c.classList.contains("reveal"));
    el.style.transitionDelay = `${Math.min(sibs.length - 1, 5) * 60}ms`;
  });
  const io = new IntersectionObserver((entries) => {
    for (const en of entries) {
      if (!en.isIntersecting) continue;
      const el = en.target;
      el.classList.add("in");
      el.addEventListener("transitionend",
        () => { el.style.transitionDelay = ""; }, { once: true });
      io.unobserve(el);
    }
  }, { rootMargin: "0px 0px -6% 0px", threshold: 0.06 });
  els.forEach((el) => io.observe(el));
}

/* ------------------------ shared demo helpers ------------------------ */

const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Run `fn` once, the first time `el` scrolls into view.
function whenVisible(el, fn) {
  if (!el) return;
  if (!("IntersectionObserver" in window)) { fn(); return; }
  const io = new IntersectionObserver((entries) => {
    for (const en of entries) {
      if (!en.isIntersecting) continue;
      io.disconnect();
      fn();
    }
  }, { threshold: 0.3 });
  io.observe(el);
}

/* --------------------------- worktree demo --------------------------- */

function initWorktreeDemo() {
  const root = document.getElementById("demo-worktree");
  if (!root) return;
  const stages = [...root.querySelectorAll(".wt-stages li")];
  const bar = root.querySelector("#wt-bar");
  const progress = root.querySelector(".wt-progress");
  const result = root.querySelector("#wt-result");
  const note = root.querySelector("#wt-note");
  const runBtn = root.querySelector("#wt-run");
  const cancelBtn = root.querySelector("#wt-cancel");
  let token = 0;
  let running = false;

  function reset() {
    token += 1;
    running = false;
    stages.forEach((s) => s.classList.remove("active", "done", "cancelled"));
    bar.style.width = "0%";
    progress.classList.remove("stalled");
    result.hidden = true;
    note.hidden = true;
    cancelBtn.disabled = true;
  }

  async function run() {
    reset();
    running = true;
    cancelBtn.disabled = false;
    const my = token;
    for (let i = 0; i < stages.length; i++) {
      if (my !== token) return;
      stages[i].classList.add("active");
      await sleep(reduced ? 60 : 720);
      if (my !== token) return;
      stages[i].classList.remove("active");
      stages[i].classList.add("done");
      bar.style.width = `${((i + 1) / stages.length) * 100}%`;
    }
    if (my !== token) return;
    result.hidden = false;
    running = false;
    cancelBtn.disabled = true;
  }

  function cancel() {
    if (!running) return;
    running = false;
    token += 1;
    const active = stages.find((s) => s.classList.contains("active"));
    if (active) {
      active.classList.remove("active");
      active.classList.add("cancelled");
    }
    progress.classList.add("stalled");
    note.hidden = false;
    cancelBtn.disabled = true;
  }

  runBtn.addEventListener("click", run);
  cancelBtn.addEventListener("click", cancel);
  whenVisible(root, () => { if (!reduced) sleep(500).then(run); });
}

/* --------------------------- skills demo --------------------------- */

function initSkillsDemo() {
  const root = document.getElementById("demo-skills");
  if (!root) return;
  const chips = [...root.querySelectorAll(".sk-chip")];
  const targets = [...root.querySelectorAll(".sk-target")];
  let token = 0;

  function reset() {
    token += 1;
    chips.forEach((c) => c.classList.remove("hl", "sent"));
    targets.forEach((tr) => tr.classList.remove("lit"));
  }

  async function run() {
    reset();
    const my = token;
    for (const chip of chips) {
      if (my !== token) return;
      chip.classList.add("hl");
      if (chip === chips[0]) {
        for (const tr of targets) {
          if (my !== token) return;
          await sleep(reduced ? 40 : 340);
          tr.classList.add("lit");
          tr.querySelector(".sk-status").textContent = "＋ symlink";
        }
      } else {
        // already-lit targets re-echo the sync with a brief flash
        targets.forEach((tr) => {
          tr.classList.remove("lit");
          void tr.offsetWidth;
          tr.classList.add("lit");
        });
        await sleep(reduced ? 40 : 420);
      }
      if (my !== token) return;
      chip.classList.remove("hl");
      chip.classList.add("sent");
    }
  }

  whenVisible(root, () => { if (!reduced) sleep(700).then(run); });

  root.addEventListener("click", (e) => {
    if (e.target.closest(".sk-chip")) run();
  });
}

/* --------------------------- reveal demo --------------------------- */

const RV_CODE = "pub fn pace(d: Duration) -> Duration {\n    d.clamp(MIN_STEP, MAX_STEP)\n}";
const RV_CODE_HL =
  '<span class="k">pub</span> <span class="k">fn</span> <span class="f">pace</span>' +
  '(d: <span class="t">Duration</span>) -&gt; <span class="t">Duration</span> {\n' +
  '    d.<span class="f">clamp</span>(MIN_STEP, MAX_STEP)\n}';

function graphemes(str) {
  if (typeof Intl !== "undefined" && Intl.Segmenter) {
    const seg = new Intl.Segmenter(lang, { granularity: "grapheme" });
    return [...seg.segment(str)].map((s) => s.segment);
  }
  return [...str];
}

function initRevealDemo() {
  const bubble = document.getElementById("rv-bubble");
  if (!bubble) return;

  let token = 0;

  function makeCaret() {
    const c = document.createElement("span");
    c.className = "dm-caret";
    return c;
  }

  // Reveal `text` into `el` grapheme by grapheme; resolves when done.
  // `finish` swaps in the final markup (markdown re-parse / highlight).
  function revealInto(el, text, finish, my, cps) {
    return new Promise((resolve) => {
      const chars = graphemes(text);
      el.appendChild(document.createTextNode(""));
      const caret = makeCaret();
      el.appendChild(caret);
      let i = 0;
      function tick() {
        if (my !== token) { resolve(); return; }
        const step = Math.max(1, Math.round(chars.length / cps));
        const end = Math.min(i + step, chars.length);
        caret.before(document.createTextNode(chars.slice(i, end).join("")));
        i = end;
        if (i >= chars.length) {
          finish(el);
          resolve();
          return;
        }
        setTimeout(tick, 1000 / cps);
      }
      if (reduced) {
        el.textContent = text;
        finish(el);
        resolve();
        return;
      }
      tick();
    });
  }

  async function run() {
    token += 1;
    const my = token;
    bubble.innerHTML = "";

    // paragraph 1 — raw markdown during reveal, bold after the "re-parse"
    const p1 = document.createElement("p");
    bubble.appendChild(p1);
    await revealInto(p1, t("rv.s1"), (el) => {
      el.innerHTML = t("rv.s1").replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
    }, my, 52);
    if (my !== token) return;
    await sleep(reduced ? 0 : 240);
    if (my !== token) return;

    // code block — plain while streaming, highlighted when the fence closes
    const pre = document.createElement("div");
    pre.className = "rv-code";
    const code = document.createElement("code");
    pre.appendChild(code);
    bubble.appendChild(pre);
    await revealInto(code, RV_CODE, (el) => { el.innerHTML = RV_CODE_HL; }, my, 110);
    if (my !== token) return;
    await sleep(reduced ? 0 : 240);
    if (my !== token) return;

    // paragraph 2
    const p2 = document.createElement("p");
    bubble.appendChild(p2);
    await revealInto(p2, t("rv.s2"), () => {}, my, 52);
  }

  const replay = document.getElementById("rv-replay");
  if (replay) replay.addEventListener("click", run);
  whenVisible(bubble.closest(".rv-demo"), () => sleep(400).then(run));
}

/* --------------------------- composer demo --------------------------- */

const CP_ITEMS = {
  slash: [
    { n: "/review", d: { ko: "PR 리뷰 체크리스트", en: "PR review checklist" }, src: "workspace" },
    { n: "/release", d: { ko: "릴리스 체크리스트", en: "release checklist" }, src: "global" },
    { n: "/explain", d: { ko: "커서 주변 코드 설명", en: "explain the code near the cursor" }, src: "global" },
    { n: "/compact", d: { ko: "대화 요약 압축", en: "compact the conversation" }, src: "global" },
  ],
  at: [
    { n: "src/lib.rs", tag: "rs" },
    { n: "src/protocol.rs", tag: "rs" },
    { n: "docs/protocol-v2.md", tag: "md" },
    { n: "config.example.toml", tag: "toml" },
    { n: "npm/client.mjs", tag: "mjs" },
    { n: "python/damon/client.py", tag: "py" },
  ],
  bang: [
    { n: { ko: "커밋 메시지", en: "commit message" }, d: { ko: "컨벤셔널 커밋으로 작성", en: "conventional commits style" }, src: "store" },
    { n: { ko: "코드리뷰 — 직설 모드", en: "code review — blunt" }, d: { ko: "거침없는 리뷰 어조", en: "no-nonsense review tone" }, src: "workspace" },
    { n: { ko: "주간 회고", en: "weekly retro" }, d: { ko: "회고 템플릿으로 요약", en: "summarize with the retro template" }, src: "store" },
  ],
};

function itemName(item) {
  return typeof item.n === "string" ? item.n : item.n[lang];
}

function initComposerDemo() {
  const root = document.getElementById("demo-composer");
  if (!root) return;
  const input = root.querySelector("#cp-input");
  const picker = root.querySelector("#cp-picker");
  const list = root.querySelector("#cp-list");
  const kindEl = root.querySelector("#cp-kind");
  const sendBtn = root.querySelector("#cp-send");
  const flash = root.querySelector("#cp-flash");
  let mode = null; // "slash" | "at" | "bang" | null
  let sel = 0;
  let tourToken = 0;
  let tourRunning = false;

  function itemsFor(m) {
    if (!m) return [];
    const q = input.value.slice(1).trim().toLowerCase();
    const items = CP_ITEMS[m];
    if (!q) return items;
    return items.filter((it) => itemName(it).toLowerCase().includes(q));
  }

  function closePicker() {
    mode = null;
    picker.hidden = true;
  }

  function renderPicker() {
    const m = mode;
    if (!m) { closePicker(); return; }
    const items = itemsFor(m);
    kindEl.textContent = m === "slash" ? "/ commands" : m === "at" ? "@ files" : "! prompts";
    list.innerHTML = "";
    if (!items.length) {
      const li = document.createElement("li");
      li.className = "pd";
      li.style.cursor = "default";
      li.textContent = t("cp.empty");
      list.appendChild(li);
    }
    items.forEach((it, i) => {
      const li = document.createElement("li");
      if (i === sel) li.classList.add("sel");
      const pn = document.createElement("span");
      pn.className = "pn";
      pn.textContent = itemName(it);
      li.appendChild(pn);
      if (it.d) {
        const pd = document.createElement("span");
        pd.className = "pd";
        pd.textContent = it.d[lang];
        li.appendChild(pd);
      } else {
        const pd = document.createElement("span");
        pd.className = "pd";
        li.appendChild(pd);
      }
      if (it.tag) {
        const pt = document.createElement("span");
        pt.className = "pt";
        pt.textContent = `.${it.tag}`;
        li.appendChild(pt);
      }
      if (it.src) {
        const ps = document.createElement("span");
        ps.className = `ps ${it.src}`;
        ps.textContent = it.src;
        li.appendChild(ps);
      }
      li.addEventListener("click", () => pick(it));
      list.appendChild(li);
    });
    picker.hidden = false;
  }

  function showFlash(text) {
    flash.hidden = false;
    flash.textContent = text;
    const fresh = flash; // restart the CSS animation
    fresh.style.animation = "none";
    void fresh.offsetWidth;
    fresh.style.animation = "";
    setTimeout(() => { flash.hidden = true; }, 1500);
  }

  function pick(it) {
    if (mode === "bang") {
      input.value = "";
      showFlash(`⚡ ${t("cp.sent")} — ${itemName(it)}`);
    } else {
      const prefix = mode === "at" ? "@" : "";
      input.value = `${prefix}${itemName(it)} `;
      input.focus();
    }
    closePicker();
  }

  function detectMode(v) {
    if (v.startsWith("/")) return "slash";
    if (v.startsWith("@")) return "at";
    if (v.startsWith("!")) return "bang";
    return null;
  }

  input.addEventListener("input", () => {
    stopTour();
    const m = detectMode(input.value);
    if (m !== mode) { mode = m; sel = 0; }
    if (!mode) { closePicker(); return; }
    renderPicker();
  });
  input.addEventListener("keydown", (e) => {
    stopTour();
    if (!mode) return;
    const items = itemsFor(mode);
    if (e.key === "ArrowDown") {
      e.preventDefault();
      sel = Math.min(sel + 1, items.length - 1);
      renderPicker();
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      sel = Math.max(sel - 1, 0);
      renderPicker();
    } else if (e.key === "Enter" && mode && items.length) {
      e.preventDefault();
      pick(items[Math.min(sel, items.length - 1)]);
    } else if (e.key === "Escape") {
      closePicker();
    }
  });

  for (const keyBtn of root.querySelectorAll(".dm-key")) {
    keyBtn.addEventListener("click", () => {
      stopTour();
      input.value = keyBtn.dataset.k;
      mode = detectMode(input.value);
      sel = 0;
      renderPicker();
      input.focus();
    });
  }

  sendBtn.addEventListener("click", () => {
    stopTour();
    if (!input.value.trim()) return;
    sendBtn.classList.add("ok");
    setTimeout(() => sendBtn.classList.remove("ok"), 900);
    input.value = "";
    closePicker();
  });

  function stopTour() {
    if (tourRunning) tourRunning = false;
    tourToken += 1;
  }

  // Scripted tour: / → @ → !, with real keystrokes into the input.
  async function tour() {
    stopTour();
    tourRunning = true;
    const my = ++tourToken;
    const alive = () => tourRunning && my === tourToken;

    async function typeText(str) {
      input.value = "";
      for (const ch of str) {
        if (!alive()) return false;
        input.value += ch;
        const m = detectMode(input.value);
        if (m !== mode) { mode = m; sel = 0; }
        if (mode) renderPicker();
        await sleep(reduced ? 0 : 85);
      }
      return alive();
    }

    closePicker();
    if (!await typeText("/rev")) return;
    await sleep(reduced ? 0 : 620); if (!alive()) return;
    pick(CP_ITEMS.slash[0]);
    await sleep(reduced ? 0 : 760); if (!alive()) return;

    if (!await typeText("@src")) return;
    await sleep(reduced ? 0 : 620); if (!alive()) return;
    pick(CP_ITEMS.at[0]);
    await sleep(reduced ? 0 : 760); if (!alive()) return;

    if (!await typeText(lang === "ko" ? "!커밋" : "!comm")) return;
    await sleep(reduced ? 0 : 620); if (!alive()) return;
    pick(CP_ITEMS.bang[0]);
    await sleep(reduced ? 0 : 900); if (!alive()) return;

    input.value = "";
    closePicker();
    tourRunning = false;
  }

  whenVisible(root, () => { if (!reduced) sleep(600).then(tour); });
}

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
    { name: { ko: "Pi", en: "Pi" },
      desc: { ko: "RPC 모드 백엔드 (omp와 같은 와이어)", en: "RPC-mode backend (omp's wire)" },
      color: "#c4b5fd", target: "#features" },
    { name: { ko: "Qwen Code", en: "Qwen Code" },
      desc: { ko: "stream-json 백엔드 (Claude형 컨트롤 플레인)", en: "stream-json backend (Claude-shaped control plane)" },
      color: "#67e8f9", target: "#features" },
    { name: { ko: "Droid", en: "Droid" },
      desc: { ko: "Factory JSON-RPC 백엔드", en: "Factory JSON-RPC backend" },
      color: "#8ab4f8", target: "#features" },
    { name: { ko: "OpenCode", en: "OpenCode" },
      desc: { ko: "HTTP+SSE 백엔드 (serve 입양/스폰)", en: "HTTP+SSE backend (adopts or spawns serve)" },
      color: "#34d399", target: "#features" },
    { name: { ko: "MiMo", en: "MiMo" },
      desc: { ko: "OpenCode 호환 HTTP+SSE 백엔드", en: "OpenCode-compatible HTTP+SSE backend" },
      color: "#a3e635", target: "#features" },
    { name: { ko: "ZCode", en: "ZCode" },
      desc: { ko: "Z.ai app-server 백엔드", en: "Z.ai app-server backend" },
      color: "#f59e0b", target: "#features" },
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

const AGENT_R = 4.8;
const CLIENT_R = 6.0;

function roundRectPath(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function makeLabelSprite(THREE, text, accent, baseH = 0.62) {
  const s = 2; // supersample for crisp text
  const font = `650 ${14 * s}px -apple-system, BlinkMacSystemFont, "Segoe UI", "Apple SD Gothic Neo", "Noto Sans KR", sans-serif`;
  const c = document.createElement("canvas");
  let ctx = c.getContext("2d");
  ctx.font = font;
  const tw = Math.ceil(ctx.measureText(text).width);
  c.width = tw + 30 * s;
  c.height = 24 * s;
  ctx = c.getContext("2d");
  ctx.font = font;
  ctx.clearRect(0, 0, c.width, c.height);
  ctx.fillStyle = "rgba(9, 13, 22, 0.74)";
  ctx.strokeStyle = accent;
  ctx.lineWidth = 1.0 * s;
  roundRectPath(ctx, 1 * s, 1 * s, c.width - 2 * s, c.height - 2 * s, 11 * s);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = "#f2f6fc";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(text, c.width / 2, c.height / 2 + s);
  const tex = new THREE.CanvasTexture(c);
  tex.colorSpace = THREE.SRGBColorSpace;
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({
    map: tex, transparent: true, depthTest: false,
  }));
  sprite.renderOrder = 20;
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

  const motion = reduced ? 0 : 1;

  renderer.setPixelRatio(Math.min(devicePixelRatio || 1, 2));
  renderer.setSize(container.clientWidth, container.clientHeight);
  container.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  scene.fog = new THREE.Fog(0x070b13, 26, 60);

  const camera = new THREE.PerspectiveCamera(
    42, container.clientWidth / container.clientHeight, 0.1, 120);
  camera.position.set(5.5, 4.5, 28);

  const controls = new OrbitControls(camera, renderer.domElement);
  // Target far left of the system center: the whole orbit renders in the
  // right half of the hero, clear of the copy column (mask finishes the job).
  controls.target.set(-9.3, 0.3, 0);
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

  function addNode(def, parent, geom, baseScale, labelH) {
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
    const label = makeLabelSprite(THREE, def.name[lang], def.color, labelH);
    label.position.y = 1.15 * baseScale;
    mesh.add(label);
    def.label = label;
    def.mesh = mesh;
    parent.add(mesh);
    pickables.push(mesh);
    return mesh;
  }

  NODES.agents.forEach((def) =>
    addNode(def, agentPivot, new THREE.SphereGeometry(0.42, 24, 24), 0.95, 0.5));
  NODES.clients.forEach((def) =>
    addNode(def, clientPivot, new THREE.IcosahedronGeometry(0.36, 0), 0.72, 0.55));

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

  function showTip(node) {
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
    if (hovered) showTip(hovered);
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
      const a = time * 0.14 * motion + (i * Math.PI * 2) / NODES.agents.length;
      n.mesh.position.set(Math.cos(a) * AGENT_R, 0, Math.sin(a) * AGENT_R);
    });
    NODES.clients.forEach((n, i) => {
      const a = -time * 0.09 * motion + (i * Math.PI * 2) / NODES.clients.length;
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

/* ------------------------------- boot ------------------------------- */

initWorktreeDemo();
initSkillsDemo();
initRevealDemo();
initComposerDemo();
applyLang();
initScene();
