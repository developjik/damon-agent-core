/**
 * Type declarations for `damon-agent` — the dependency-free Node client
 * for a Damon daemon (JSON-RPC v2 over WebSocket, optional E2E relay).
 *
 * The shapes mirror docs/protocol-v2.md: `StreamEvent` payloads are the
 * daemon's typed event model; consumers usually switch on
 * `event.type`.
 */

/** One stdio MCP server forwarded into a session. */
export interface McpServerConfig {
  command: string;
  args?: string[];
  env?: Record<string, string>;
}

/** A persistence handle pointing at a backend's own durable session. */
export interface PersistenceHandle {
  provider: string;
  native_handle: string;
  metadata?: unknown;
}

/** Usage snapshot reported at turn boundaries. */
export interface Usage {
  input_tokens?: number;
  cached_input_tokens?: number;
  output_tokens?: number;
  total_tokens?: number;
  context_used?: number;
  context_window?: number;
  cost_usd?: number;
}

/** A normalized stream event — `type` is the discriminator
 *  (snake_case, e.g. "timeline", "turn_completed", "permission_requested"). */
export interface StreamEvent {
  turn_id?: string | null;
  type: string;
  [field: string]: unknown;
}

/** An action the agent offered on a permission ask. */
export interface PermissionAction {
  id: string;
  label: string;
  behavior: "allow" | "deny";
  variant?: string;
}

/** A permission ask raised by the backend. */
export interface PermissionRequest {
  id: string;
  kind: string;
  name: string;
  title?: string;
  input?: unknown;
  detail?: unknown;
  actions: PermissionAction[];
}

/** `permission.respond` payload. */
export interface PermissionResponse {
  behavior: "allow" | "deny";
  action_id?: string;
  updated_input?: unknown;
  answer?: string;
  message?: string;
  interrupt?: boolean;
}

/** Events yielded by {@link DamonClient.events}. */
export type ClientEvent =
  | {
      type: "event";
      sessionId: string;
      event: StreamEvent;
      /** true for catch-up frames the daemon replays after a
       *  (re)subscribe — history the consumer may have rendered
       *  already. */
      replay: boolean;
    }
  | {
      /** turn.start outcome, ordered after that session's events. */
      type: "promptDone";
      sessionId: string;
      result?: { turnId: string; stopReason: string; usage?: Usage };
      error?: { code?: number; message?: string };
    }
  | { type: "connected" }
  | { type: "reconnected" }
  | { type: "disconnected" };

/** Options for {@link DamonClient.connect}. */
export interface ConnectOptions {
  /** Bearer token; a single-use ticket is fetched and the token never
   *  appears in a URL. */
  token?: string;
  /** WebSocket implementation — Node >= 22's global works; tests inject
   *  a mock. */
  webSocket?: unknown;
  fetchImpl?: typeof fetch;
  /** Re-issue session.resume for every touched session after a
   *  reconnect (default true) — subscriptions survive daemon
   *  restarts; replays arrive flagged. */
  autoResume?: boolean;
}

/** Options for {@link connectRelay}. */
export interface RelayConnectOptions {
  /** ws(s)://host:port of a damon-relay. */
  url: string;
  /** The daemon's registered relay name. */
  name: string;
  /** The daemon's auth_token — also the E2E shared secret. */
  token: string;
  webSocket?: unknown;
  handshakeTimeoutMs?: number;
  autoResume?: boolean;
}

/** One session row from `session.list`. */
export interface SessionRow {
  sessionId: string;
  createdAt: string;
  backend: string;
  title: string;
  cwd: string;
  tags: string[];
  pinned: boolean;
  archived: boolean;
}

/** One stored message row. */
export interface StoredMessage {
  id: number;
  session_id: string;
  role: string;
  ts: number;
  data: { content?: string; [field: string]: unknown };
}

/** A native session importable from a backend. */
export interface ImportableSession {
  handle: PersistenceHandle;
  title?: string | null;
  cwd?: string | null;
  modified_at?: number | null;
}

/** A model a backend offers. */
export interface ModelDef {
  id: string;
  name: string;
  selectable: boolean;
}

/** A permission/behavior mode a backend offers. */
export interface ModeDef {
  id: string;
  name: string;
  description?: string | null;
}

/** JSON-RPC error thrown by every call method. */
export class RpcError extends Error {
  /** Typed code — Damon's own range is -32000..-32099
   *  (e.g. -32001 session-not-live, -32006 turn-in-progress). */
  code: number;
  constructor(code: number, message: string);
}

export class DamonClient {
  /** Latest {"hello": …} greeting (resent on every reconnect). */
  serverHello: {
    protocol: number;
    daemon: string;
    version: string;
    backends: string[];
    methods: unknown[];
    permissionTimeoutSecs?: number;
  } | null;

  constructor(io: unknown, dial: () => Promise<unknown>, opts?: { autoResume?: boolean });

  /** Connect over plain (or TLS) WebSocket. Redials with backoff on
   *  link death; calls made while down wait up to 10s for the link. */
  static connect(url: string, opts?: ConnectOptions): Promise<DamonClient>;

  /** Connect through a damon-relay (X25519 + AES-256-GCM E2E). */
  static connectRelay(opts: RelayConnectOptions): Promise<DamonClient>;

  /** Async iterator over daemon events; ends only after close(). */
  events(): AsyncGenerator<ClientEvent, void, unknown>;

  /** Handshake — {protocol, daemon, version, backends[], methods[]}. */
  hello(): Promise<Record<string, unknown>>;

  /** Create a session; returns the sessionId. */
  newSession(cwd: string, backend?: string): Promise<string>;

  /** List sessions (paged). */
  listSessions(opts?: { limit?: number; offset?: number }): Promise<SessionRow[]>;

  /** Session history rows (paged). */
  sessionMessages(sessionId: string, opts?: { limit?: number; offset?: number }): Promise<StoredMessage[]>;

  /** Resume an existing session; throws (RpcError) if unknown. */
  resumeSession(sessionId: string): Promise<string>;

  /** Resume a native session by handle (the import path). */
  resumeByHandle(
    handle: PersistenceHandle,
    opts?: { title?: string; cwd?: string }
  ): Promise<string>;

  /** Sessions importable from a backend's native store. */
  importSessions(backend: string, cwd?: string): Promise<ImportableSession[]>;

  /** Fork a session into a new one, optionally up to a message id. */
  forkSession(sessionId: string, uptoMessageId?: number): Promise<string>;

  deleteSession(sessionId: string): Promise<Record<string, never>>;

  renameSession(sessionId: string, title: string): Promise<Record<string, never>>;

  /** Full-text search — [{sessionId, messageId, snippet}]. */
  search(query: string, limit?: number): Promise<
    Array<{ sessionId: string; messageId: number; snippet: string }>
  >;

  /** Usage: one session's totals, or per-model rollups when omitted. */
  usage(sessionId?: string): Promise<Record<string, unknown>>;

  /** Start a turn; resolves at turn end with
   *  {turnId, stopReason, usage?}. With `{detach: true}` resolves
   *  immediately {turnId, detached: true} — the outcome arrives as
   *  stream events / replay instead. `timeoutSecs` bounds the turn. */
  prompt(
    sessionId: string,
    text: string | Array<{ type: string; text: string }>,
    timeoutSecs?: number,
    opts?: { detach?: boolean }
  ): Promise<{ turnId: string; stopReason?: string; usage?: Usage; detached?: boolean }>;

  cancel(sessionId: string): Promise<{ cancelled: boolean }>;

  steer(sessionId: string, prompt: string, expectedTurn?: string): Promise<{ result: string }>;

  setModel(sessionId: string, model: string): Promise<Record<string, never>>;

  setMode(sessionId: string, mode: string): Promise<Record<string, never>>;

  /** Registered backends with availability + capabilities. */
  backends(): Promise<{
    backends: Array<{
      id: string;
      available: boolean;
      capabilities: Record<string, boolean>;
    }>;
  }>;

  /** A backend's model/mode catalog. */
  catalog(backend: string): Promise<{ models: ModelDef[]; modes: ModeDef[] }>;

  /** Answer a permission_requested event. */
  respondPermission(
    sessionId: string,
    requestId: string,
    response: PermissionResponse
  ): Promise<Record<string, never>>;

  /** Stop reconnecting and end events() iterators. */
  close(): void;
}

/** Connect through a damon-relay — E2E-encrypted, identical surface. */
export function connectRelay(opts: RelayConnectOptions): Promise<DamonClient>;
