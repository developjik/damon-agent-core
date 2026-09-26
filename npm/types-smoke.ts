/**
 * Types smoke — compiled by `npm run typecheck` (tsc --noEmit). Every
 * public shape from client.d.ts is exercised at the type level only;
 * runtime never imports this file.
 */
import { connectRelay, DamonClient, type ClientEvent, type PermissionResponse } from "./client.mjs";

async function main(url: string): Promise<void> {
  const client = await DamonClient.connect(url, { token: "t", autoResume: false });
  const sid = await client.newSession("/tmp", "claude");
  await client.prompt(sid, "hello", 30, { detach: false });
  await client.prompt(sid, [{ type: "text", text: "blocks" }]);

  for await (const ev of client.events()) {
    if (ev.type === "event") {
      const kind: string = ev.event.type;
      if (ev.replay || kind === "timeline") continue;
      if (kind === "permission_requested") {
        const resp: PermissionResponse = { behavior: "allow" };
        await client.respondPermission(ev.sessionId, "req-1", resp);
      }
    } else if (ev.type === "promptDone") {
      const usage = ev.result?.usage?.cost_usd ?? 0;
      console.log(String(usage));
    }
  }

  const rows = await client.listSessions({ limit: 10 });
  const pinned: boolean = rows[0]?.pinned ?? false;
  const msgs = await client.sessionMessages(sid, { limit: 5 });
  const firstRole: string = msgs[0]?.role ?? "";
  await client.resumeSession(sid);
  await client.resumeByHandle(
    { provider: "claude", native_handle: "abc" },
    { title: "t", cwd: "/w" }
  );
  await client.forkSession(sid, 42);
  await client.renameSession(sid, "name");
  const hits = await client.search("error*", 5);
  console.log(hits[0]?.snippet ?? "", pinned, firstRole);
  await client.usage(sid);
  await client.cancel(sid);
  await client.steer(sid, "more", "t1");
  await client.setModel(sid, "sonnet");
  await client.setMode(sid, "plan");
  const backends = await client.backends();
  console.log(backends.backends.map((b) => b.id).join(","));
  const catalog = await client.catalog("claude");
  console.log(catalog.models[0]?.name ?? "");
  client.close();

  const relay: DamonClient = await connectRelay({
    url: "ws://relay:8080",
    name: "mydaemon",
    token: "tok",
    handshakeTimeoutMs: 5000,
  });
  relay.close();
}

// ClientEvent union narrowing compiles.
const _narrow = (ev: ClientEvent): "up" | "down" => {
  if (ev.type === "connected" || ev.type === "reconnected") return "up";
  return "down";
};
void _narrow;

export { main };
