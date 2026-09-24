//! Remote relay: a public `damon-relay` server pipes WebSocket frames between
//! a registered daemon and connecting clients. The daemon dials OUT to the
//! relay (no inbound port needed); clients dial the relay by daemon name.
//!
//! E2E: the client opens an X25519 handshake over the relay with its
//! ephemeral public key; the daemon answers with its own key, the client
//! proves sha256(token || client_pub || daemon_pub) FIRST, and only then
//! the daemon answers with sha256(token || daemon_pub || client_pub).
//! Proving client-first means the daemon never exposes token-derived
//! material to an unauthenticated peer. NOTE: the relay itself (and any
//! observer on a plain ws:// link) sees the client's proof and both
//! public keys — a low-entropy auth_token is offline-brute-forcible from
//! one captured handshake. Use a high-entropy token and wss:// relays.
//! Post-handshake frames are AES-256-GCM with direction-separated
//! keys and a sequence number as nonce — the relay sees only ciphertext.
//!
//! Registration: the daemon dials `/register?name=X` and sends
//! `{"auth": secret}` as its first frame (never a URL query — queries
//! leak into logs). The optional secret defeats name squatting.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::Digest;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, warn};

use crate::api::AppState;
use crate::config::SecretRef;

// ---------------------------------------------------------------------------
// E2E crypto
// ---------------------------------------------------------------------------

/// An established E2E session: encrypt/decrypt JSON text frames.
///
/// Send and receive use separate ciphers (`d2c` vs `c2d`) and every frame
/// carries a big-endian sequence number that doubles as the AEAD nonce.
/// A replayed, reordered, or injected frame fails `decrypt`; callers treat
/// any decrypt error as fatal and close the session (fail-closed).
pub struct E2e {
    send_cipher: aes_gcm::Aes256Gcm,
    send_seq: AtomicU64,
    recv_cipher: aes_gcm::Aes256Gcm,
    recv_seq: AtomicU64,
}

impl E2e {
    /// Daemon side: read the client's {e2e_pub}, answer {e2e_pub}, then
    /// verify the client's proof BEFORE emitting our own. The daemon
    /// never reveals token-derived material to an unauthenticated peer —
    /// an attacker connecting as a fake client cannot harvest
    /// (pubkey, proof) samples to brute the auth_token offline.
    pub async fn daemon_handshake(
        tx: &mpsc::Sender<String>,
        rx: &mut mpsc::Receiver<String>,
        token: &str,
    ) -> anyhow::Result<Self> {
        // 1. Client's ephemeral public key.
        let msg = rx.recv().await.context("client handshake missing")?;
        let v: Value = serde_json::from_str(&msg)?;
        let client_pub = B64
            .decode(v["e2e_pub"].as_str().context("missing e2e_pub")?)
            .context("bad e2e_pub")?;
        let client_pub: [u8; 32] = client_pub
            .try_into()
            .map_err(|_| anyhow::anyhow!("bad pubkey len"))?;

        // 2. Our keypair — public key only, no proof yet.
        let secret = x25519_dalek::StaticSecret::random_from_rng(getrandom_rng());
        let public = x25519_dalek::PublicKey::from(&secret);
        tx.send(json!({"e2e_pub": B64.encode(public.as_bytes())}).to_string())
            .await
            .context("send handshake")?;

        // 3. Client proves token knowledge first; only a verified client
        //    ever sees our proof.
        let msg = rx.recv().await.context("client proof missing")?;
        let v: Value = serde_json::from_str(&msg)?;
        let their_proof = v["e2e_proof"].as_str().context("missing e2e_proof")?;
        if !crate::config::constant_time_eq(
            their_proof.as_bytes(),
            proof(token, &client_pub, public.as_bytes()).as_bytes(),
        ) {
            // Tell the client WHY (it learns nothing it doesn't know —
            // its own proof failed) so it fails fast instead of waiting
            // out the handshake timeout.
            let _ = tx.send(json!({"e2e_error": "auth"}).to_string()).await;
            bail!("client failed E2E proof — wrong auth_token?");
        }
        tx.send(json!({"e2e_proof": proof(token, public.as_bytes(), &client_pub)}).to_string())
            .await
            .context("send proof")?;
        let shared = secret.diffie_hellman(&x25519_dalek::PublicKey::from(client_pub));
        // A low-order public key yields an all-zero shared secret —
        // predictable session keys. Only an authenticated peer could
        // pull this off (the proof binds pubkeys), but check anyway.
        anyhow::ensure!(shared.was_contributory(), "non-contributory DH share");
        Ok(Self::from_shared(shared.as_bytes(), true))
    }

    /// Client side: send {e2e_pub}, receive the daemon's bare {e2e_pub},
    /// prove token knowledge FIRST, then verify the daemon's proof. The
    /// daemon answers its proof only after authenticating us, so a relay
    /// or name-squatter cannot collect proofs for chosen pubkeys.
    pub async fn client_handshake(
        tx: &mpsc::Sender<String>,
        rx: &mut mpsc::Receiver<String>,
        token: &str,
    ) -> anyhow::Result<Self> {
        // 1. Our ephemeral public key.
        let secret = x25519_dalek::StaticSecret::random_from_rng(getrandom_rng());
        let public = x25519_dalek::PublicKey::from(&secret);
        tx.send(json!({"e2e_pub": B64.encode(public.as_bytes())}).to_string())
            .await
            .context("send handshake")?;

        // 2. Daemon's bare {e2e_pub} — no proof until we authenticate.
        let msg = rx.recv().await.context("daemon handshake missing")?;
        let v: Value = serde_json::from_str(&msg)?;
        // A relay-level refusal ({"error": ...} forwarded as e2e_error)
        // or a daemon rejection fails fast with the reason instead of a
        // generic missing-field error.
        if let Some(e) = v["e2e_error"].as_str() {
            bail!("relay/daemon refused handshake: {e}");
        }
        let their_pub = B64
            .decode(v["e2e_pub"].as_str().context("missing e2e_pub")?)
            .context("bad e2e_pub")?;
        let their_pub: [u8; 32] = their_pub
            .try_into()
            .map_err(|_| anyhow::anyhow!("bad pubkey len"))?;

        // 3. Our proof over (client_pub, daemon_pub).
        tx.send(json!({"e2e_proof": proof(token, public.as_bytes(), &their_pub)}).to_string())
            .await
            .context("send proof")?;

        // 4. Daemon's proof, only sent to an authenticated client — or an
        //    explicit auth rejection so we fail fast instead of timing out.
        let msg = rx.recv().await.context("daemon proof missing")?;
        let v: Value = serde_json::from_str(&msg)?;
        if let Some(e) = v["e2e_error"].as_str() {
            if e == "auth" {
                bail!("daemon rejected our proof — wrong auth_token?");
            }
            bail!("relay/daemon refused handshake: {e}");
        }
        let their_proof = v["e2e_proof"].as_str().context("missing e2e_proof")?;
        if !crate::config::constant_time_eq(
            their_proof.as_bytes(),
            proof(token, &their_pub, public.as_bytes()).as_bytes(),
        ) {
            bail!("daemon failed E2E proof — wrong auth_token?");
        }
        let shared = secret.diffie_hellman(&x25519_dalek::PublicKey::from(their_pub));
        // A low-order public key yields an all-zero shared secret —
        // predictable session keys. Only an authenticated peer could
        // pull this off (the proof binds pubkeys), but check anyway.
        anyhow::ensure!(shared.was_contributory(), "non-contributory DH share");
        Ok(Self::from_shared(shared.as_bytes(), false))
    }

    fn from_shared(shared: &[u8; 32], daemon_side: bool) -> Self {
        use aes_gcm::KeyInit;
        // Direction-separated keys straight from the DH output: a frame
        // reflected back to its sender can never decrypt under the other
        // direction's key.
        let d2c = derive_key(shared, b"d2c");
        let c2d = derive_key(shared, b"c2d");
        let (send_key, recv_key) = if daemon_side { (d2c, c2d) } else { (c2d, d2c) };
        Self {
            send_cipher: aes_gcm::Aes256Gcm::new_from_slice(&send_key).expect("32-byte key"),
            send_seq: AtomicU64::new(0),
            recv_cipher: aes_gcm::Aes256Gcm::new_from_slice(&recv_key).expect("32-byte key"),
            recv_seq: AtomicU64::new(0),
        }
    }

    /// Encrypt a JSON text frame → base64 of `seq(8B) || nonce(12B) || ct`.
    /// Plaintext is capped at [`crate::rpc::MAX_RESPONSE_BYTES`]: base64
    /// inflates ~4/3 and the relay wraps the ciphertext in a JSON
    /// envelope, so a larger plaintext would produce a frame over the
    /// 4 MiB socket cap that the peer then drops silently.
    pub fn encrypt(&self, plaintext: &str) -> anyhow::Result<String> {
        use aes_gcm::aead::Aead;
        anyhow::ensure!(
            plaintext.len() <= crate::rpc::MAX_RESPONSE_BYTES,
            "plaintext {} bytes exceeds the {}-byte relay frame budget",
            plaintext.len(),
            crate::rpc::MAX_RESPONSE_BYTES
        );
        let seq = self.send_seq.fetch_add(1, Ordering::Relaxed);
        let nonce = seq_nonce(seq);
        let ct = self
            .send_cipher
            .encrypt(aes_gcm::Nonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| anyhow::anyhow!("encrypt failed"))?;
        let mut frame = Vec::with_capacity(20 + ct.len());
        frame.extend_from_slice(&seq.to_be_bytes());
        frame.extend_from_slice(&nonce);
        frame.extend_from_slice(&ct);
        Ok(B64.encode(frame))
    }

    /// Decrypt a base64 frame → JSON text. The frame's sequence number must
    /// equal the next expected value — anything else means a replayed,
    /// reordered, or injected frame and the caller closes the session.
    pub fn decrypt(&self, b64: &str) -> anyhow::Result<String> {
        use aes_gcm::aead::Aead;
        let frame = B64.decode(b64).context("bad base64")?;
        if frame.len() < 20 {
            bail!("frame too short");
        }
        let seq = u64::from_be_bytes(frame[..8].try_into().unwrap());
        let expected = self.recv_seq.load(Ordering::Relaxed);
        if seq != expected {
            bail!("frame seq {seq} != expected {expected} — replay or reorder");
        }
        let pt = self
            .recv_cipher
            .decrypt(aes_gcm::Nonce::from_slice(&frame[8..20]), &frame[20..])
            .map_err(|_| anyhow::anyhow!("decrypt failed — wrong key or tampered"))?;
        self.recv_seq.store(expected + 1, Ordering::Relaxed);
        Ok(String::from_utf8(pt)?)
    }
}

/// sha256(token || mine || theirs) — binds a handshake proof to BOTH
/// public keys, so a relay cannot replay a captured proof.
fn proof(token: &str, mine: &[u8], theirs: &[u8]) -> String {
    let mut h = sha2::Sha256::new();
    h.update(token.as_bytes());
    h.update(mine);
    h.update(theirs);
    B64.encode(h.finalize())
}

/// sha256(shared || label) — directional session key derivation.
fn derive_key(shared: &[u8; 32], label: &[u8]) -> [u8; 32] {
    let mut h = sha2::Sha256::new();
    h.update(shared);
    h.update(label);
    h.finalize().into()
}

/// 12-byte AEAD nonce for a frame: 4 zero bytes || seq (big-endian).
fn seq_nonce(seq: u64) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[4..].copy_from_slice(&seq.to_be_bytes());
    n
}

fn getrandom_rng() -> impl rand_core::RngCore + rand_core::CryptoRng {
    struct R;
    impl rand_core::RngCore for R {
        fn next_u32(&mut self) -> u32 {
            let mut b = [0u8; 4];
            getrandom::fill(&mut b).unwrap();
            u32::from_le_bytes(b)
        }
        fn next_u64(&mut self) -> u64 {
            let mut b = [0u8; 8];
            getrandom::fill(&mut b).unwrap();
            u64::from_le_bytes(b)
        }
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            getrandom::fill(dest).unwrap();
        }
        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
            getrandom::fill(dest).map_err(|_| rand_core::Error::from(std::num::NonZeroU32::MIN))
        }
    }
    impl rand_core::CryptoRng for R {}
    R
}
/// Connect to `relay_url` (ws://host:port), register as `name`, and serve
/// each client that connects through the relay. Reconnects on drop.
/// `secret` is the RAW config ref (env:/keychain:/!cmd or literal) — it is
/// re-resolved per attempt so a rotated secret takes effect without a
/// restart, and a resolution failure is loud, never a silent downgrade to
/// unauthenticated registration.
pub async fn run_tunnel(
    state: Arc<AppState>,
    relay_url: String,
    name: String,
    secret: Option<String>,
) {
    // Plain ws:// sends the registration secret and the E2E handshake
    // proofs in cleartext — fine for loopback/tests, dangerous anywhere
    // else. Warn once rather than silently accepting the exposure.
    if relay_url.starts_with("ws://")
        && !relay_url[5..].starts_with("127.0.0.1")
        && !relay_url[5..].starts_with("localhost")
        && !relay_url[5..].starts_with("[::1]")
    {
        warn!(relay = %relay_url, "relay URL is plaintext ws:// — registration secret and E2E proofs cross the wire unencrypted; use wss://");
    }
    loop {
        let resolved = secret.as_deref().map(|s| match SecretRef::parse(s) {
            Ok(r) => r
                .resolve()
                .map_err(|e| anyhow::anyhow!("relay secret resolution failed: {e:#}")),
            // A value that LOOKS like a ref but failed to parse
            // ("env:", "keychain:svc", "!") is a config typo — fail
            // closed rather than registering with a guessable literal
            // (same policy as api.rs auth_token resolution).
            Err(e) if s.starts_with("env:") || s.starts_with("keychain:") || s.starts_with('!') => {
                Err(anyhow::anyhow!("relay secret ref invalid: {e:#}"))
            }
            Err(_) => Ok(s.to_string()),
        });
        let secret = match resolved {
            Some(Ok(s)) => Some(s),
            Some(Err(e)) => {
                // Configured but unresolvable: refuse THIS attempt
                // loudly — do not register unauthenticated.
                error!(error = %e, "relay secret unresolvable; skipping registration attempt");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            None => None,
        };
        match tunnel_once(&state, &relay_url, &name, secret.as_deref()).await {
            Ok(()) => info!("relay tunnel closed; reconnecting"),
            Err(e) if e.is::<NameTaken>() => {
                // Someone else holds our name — either a stale tunnel on
                // the relay or a squatter. Log loudly and back off well
                // past the relay's disconnect cleanup window.
                error!(error = %e, "relay refused registration (name in use); retrying in 60s — if this persists, another daemon or a squatter owns the name");
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                continue;
            }
            Err(e) => warn!(error = %e, "relay tunnel failed; reconnecting in 5s"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// The relay rejected our registration because the name is already live.
#[derive(Debug, thiserror::Error)]
#[error("relay name already registered")]
pub struct NameTaken;

/// Concurrent client sessions one tunnel may spawn — bounds the task and
/// channel fanout an unauthenticated relay client can cause on the daemon.
const MAX_SESSIONS: usize = 32;

/// Bound on the E2E handshake — an idle client must not pin a session
/// slot (and its place under MAX_SESSIONS) forever.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// A tunnel socket silent this long is half-open (NAT drop, dead relay)
/// — the daemon's pings keep a healthy link under it; reconnect past it.
const TUNNEL_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Ping cadence: keeps the tunnel inside the relay's idle window and
/// gives the daemon's own idle check a Pong to observe.
const TUNNEL_PING_EVERY: std::time::Duration = std::time::Duration::from_secs(30);

async fn tunnel_once(
    state: &Arc<AppState>,
    relay_url: &str,
    name: &str,
    secret: Option<&str>,
) -> anyhow::Result<()> {
    // The secret travels as the first post-upgrade frame, never a URL
    // query — query strings land in intermediary and relay logs.
    let url = format!(
        "{}/register?name={}",
        relay_url.trim_end_matches('/'),
        urlencoding(name)
    );
    // Cap inbound frames — the default ~64 MiB lets a hostile relay force
    // huge allocations into the session channels.
    let ws_cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(4 << 20))
        .max_frame_size(Some(4 << 20));
    let (ws, _) = tokio_tungstenite::connect_async_with_config(&url, Some(ws_cfg), false)
        .await
        .context("cannot reach relay")?;
    let (mut writer, mut reader) = ws.split();
    let auth = json!({ "auth": secret.unwrap_or("") }).to_string();
    writer
        .send(tokio_tungstenite::tungstenite::Message::Text(auth.into()))
        .await
        .context("cannot send relay auth frame")?;
    info!(relay = %relay_url, name = %name, "registered with relay");
    // The relay sends {"client": <id>} when a client connects, then pipes
    // that client's frames as {"client": id, "data": "..."}.
    // We send {"client": id, "data": "..."} back.
    // Sessions map client_id → (inbound channel, task abort handle) so a
    // backpressured or dead session can be dropped from the shared loop.
    let mut sessions: HashMap<u64, (mpsc::Sender<String>, tokio::task::AbortHandle)> =
        HashMap::new();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
    // Session tasks report their exit here as (client id, task id) so
    // the map entry is freed even when the client stays connected
    // (handshake fail, decrypt fail). The task id lets the done branch
    // skip a REPLACED session's late exit — it must not free the entry
    // that replaced it.
    let (done_tx, mut done_rx) = mpsc::channel::<(u64, tokio::task::Id)>(64);

    // Inbound liveness: any frame (incl. Pong to our pings) resets the
    // idle clock — a silent socket is half-open, not merely quiet.
    let mut last_rx = tokio::time::Instant::now();
    let mut ping = tokio::time::interval(TUNNEL_PING_EVERY);

    loop {
        tokio::select! {
            msg = reader.next() => {
                let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) = msg else {
                    last_rx = tokio::time::Instant::now();
                    if matches!(msg, Some(Err(_)) | None) { break; }
                    continue;
                };
                last_rx = tokio::time::Instant::now();
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Some(err) = v["error"].as_str() {
                    // Registration was refused (e.g. the name is held by
                    // another tunnel). Surface it distinctly so the
                    // reconnect loop can back off loudly.
                    if err == "name_taken" {
                        bail!(NameTaken);
                    }
                    bail!("relay refused registration: {err}");
                }
                let Some(client_id) = v["client"].as_u64() else { continue };
                if v["disconnect"].as_bool() == Some(true) {
                    // Relay says this client went away — drop the session
                    // so the map doesn't grow forever.
                    sessions.remove(&client_id);
                    continue;
                }
                if let Some(data) = v["data"].as_str() {
                    // Frame from a client. try_send, never await: a full
                    // per-session channel must not stall the shared loop —
                    // one flooding client would freeze every session.
                    if let Some((in_tx, abort)) = sessions.get(&client_id) {
                        match in_tx.try_send(data.to_string()) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_))
                            | Err(mpsc::error::TrySendError::Closed(_)) => {
                                warn!(client = client_id, "dropping backpressured relay session");
                                abort.abort();
                                sessions.remove(&client_id);
                                // Tell the relay to end the client socket
                                // too — otherwise it keeps a zombie
                                // session whose frames go nowhere.
                                let _ = out_tx.try_send(
                                    json!({"client": client_id, "disconnect": true}).to_string(),
                                );
                            }
                        }
                    }
                } else if v.get("data").is_none() {
                    // New client connected — spawn a session.
                    if sessions.len() >= MAX_SESSIONS {
                        warn!(client = client_id, cap = MAX_SESSIONS,
                              "relay session cap reached; refusing client");
                        continue;
                    }
                    let (in_tx, in_rx) = mpsc::channel::<String>(64);
                    let (sess_out, mut sess_rx) = mpsc::channel::<String>(64);
                    let state = state.clone();
                    let out_tx = out_tx.clone();
                    // Kept outside the session task: the shadowed sender
                    // above moves into it, but a replaced-session
                    // disconnect notice still needs a sender here.
                    let out_tx_dup = out_tx.clone();
                    // Pipe sess_out → relay (handshake + encrypted frames).
                    let out_tx2 = out_tx.clone();
                    tokio::spawn(async move {
                        while let Some(data) = sess_rx.recv().await {
                            let _ = out_tx2
                                .send(json!({"client": client_id, "data": data}).to_string())
                                .await;
                        }
                    });
                    let done_tx = done_tx.clone();
                    let task = tokio::spawn(async move {
                        // E2E handshake over the relay pipe, bounded: an
                        // idle client must not pin a session slot forever.
                        let mut in_rx = in_rx;
                        // Fail closed: an unresolvable or missing token
                        // must not become an empty-token handshake.
                        let token = match state.auth_token().await {
                            Some(Ok(t)) => t,
                            other => {
                                warn!(?other, "auth_token unavailable; refusing relay session");
                                return;
                            }
                        };
                        let e2e = match tokio::time::timeout(
                            HANDSHAKE_TIMEOUT,
                            E2e::daemon_handshake(&sess_out, &mut in_rx, &token),
                        )
                        .await
                        {
                            Ok(Ok(e)) => e,
                            Ok(Err(e)) => {
                                warn!(error = %e, "E2E handshake failed");
                                return;
                            }
                            Err(_) => {
                                warn!("E2E handshake timed out; dropping client");
                                return;
                            }
                        };
                        // Bridge: decrypt inbound, run handle_socket, encrypt outbound.
                        let (plain_in_tx, plain_in_rx) = mpsc::channel::<String>(64);
                        let (plain_out_tx, mut plain_out_rx) = mpsc::channel::<String>(64);
                        let e2e = Arc::new(e2e);
                        let e2e_in = e2e.clone();
                        let out_tx_in = out_tx.clone();
                        tokio::spawn(async move {
                            while let Some(ct) = in_rx.recv().await {
                                match e2e_in.decrypt(&ct) {
                                    Ok(pt) => { let _ = plain_in_tx.send(pt).await; }
                                    // Tampered or wrong-key frames: fail
                                    // closed — drop the session rather than
                                    // silently swallowing messages.
                                    Err(e) => {
                                        warn!(error = %e, "E2E decrypt failed; closing session");
                                        // Propagate the end to the relay
                                        // so it closes the client socket
                                        // instead of leaving a zombie.
                                        let _ = out_tx_in.send(
                                            json!({"client": client_id, "disconnect": true})
                                                .to_string(),
                                        ).await;
                                        return;
                                    }
                                }
                            }
                        });
                        let e2e_out = e2e.clone();
                        tokio::spawn(async move {
                            while let Some(pt) = plain_out_rx.recv().await {
                                match e2e_out.encrypt(&pt) {
                                    Ok(ct) => {
                                        let _ = out_tx.send(
                                            json!({"client": client_id, "data": ct}).to_string()
                                        ).await;
                                    }
                                    Err(e) => warn!(error = %e, "E2E encrypt failed"),
                                }
                            }
                        });
                        crate::rpc::handle_socket(plain_in_rx, plain_out_tx, state).await;
                    });
                    // A hostile or buggy relay can re-send the connect
                    // notice for an id whose session is still live.
                    // Abort the stale task and replace its entry — a
                    // plain overwrite would orphan the old task, and its
                    // exit notification would later free THIS new entry.
                    let old = sessions.insert(client_id, (in_tx, task.abort_handle()));
                    if let Some((_, old_abort)) = old {
                        warn!(client = client_id,
                              "relay re-sent connect for a live session; replacing it");
                        old_abort.abort();
                        // The replaced session is dead — tell the relay
                        // to drop the client socket so it re-attaches
                        // cleanly instead of piping into a void.
                        let _ = out_tx_dup.try_send(
                            json!({"client": client_id, "disconnect": true}).to_string(),
                        );
                    }
                    // Free the slot when the task exits for ANY reason —
                    // only the relay's disconnect notice freed it before,
                    // so a failed handshake leaked the slot forever.
                    let task_id = task.id();
                    tokio::spawn(async move {
                        let _ = task.await;
                        let _ = done_tx.send((client_id, task_id)).await;
                    });
                }
            }
            out = out_rx.recv() => {
                let Some(text) = out else { continue };
                if writer.send(tokio_tungstenite::tungstenite::Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            done = done_rx.recv() => {
                // A session task exited (handshake fail/timeout, decrypt
                // fail, or socket end) — free its slot, but only if the
                // map still holds THAT task: an aborted (replaced)
                // session's late exit must not free its replacement.
                if let Some((client_id, task_id)) = done
                    && sessions.get(&client_id).is_some_and(|(_, h)| h.id() == task_id)
                {
                    sessions.remove(&client_id);
                }
            }
            _ = ping.tick() => {
                // Keepalive: proves the socket is writable and gives the
                // relay's idle check a frame to observe.
                if writer
                    .send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            _ = tokio::time::sleep_until(last_rx + TUNNEL_IDLE_TIMEOUT) => {
                // No inbound frame for the whole window — the socket is
                // half-open; break so run_tunnel redials.
                warn!("relay tunnel silent for {}s; reconnecting", TUNNEL_IDLE_TIMEOUT.as_secs());
                break;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Client side: connect through the relay
// ---------------------------------------------------------------------------

/// Connect to `relay_url`, attach to daemon `name`, do the E2E handshake,
/// and return (tx, rx) channel pair carrying plaintext JSON frames.
pub async fn client_connect(
    relay_url: &str,
    name: &str,
    token: &str,
) -> anyhow::Result<(mpsc::Sender<String>, mpsc::Receiver<String>)> {
    let url = format!(
        "{}/connect?name={}",
        relay_url.trim_end_matches('/'),
        urlencoding(name)
    );
    // Same inbound frame cap as the tunnel — a hostile relay must not
    // force multi-MiB allocations into the channels.
    let ws_cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(4 << 20))
        .max_frame_size(Some(4 << 20));
    let (ws, _) = tokio_tungstenite::connect_async_with_config(&url, Some(ws_cfg), false)
        .await
        .context("cannot reach relay")?;
    let (mut writer, mut reader) = ws.split();

    let (raw_in_tx, mut raw_in_rx) = mpsc::channel::<String>(64);
    let (raw_out_tx, mut raw_out_rx) = mpsc::channel::<String>(64);
    let read_pump = tokio::spawn(async move {
        // Ping/Pong/Binary keep the link alive — a keepalive proxy's ping
        // must not end the pump (tungstenite answers pings itself).
        while let Some(msg) = reader.next().await {
            let t = match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            if let Ok(v) = serde_json::from_str::<Value>(&t) {
                // Relay-level refusal ({"error": ...}) — forward it as an
                // e2e_error so the handshake fails fast with the reason
                // instead of stalling on a missing e2e_pub.
                if let Some(e) = v["error"].as_str() {
                    let _ = raw_in_tx.send(json!({"e2e_error": e}).to_string()).await;
                    continue;
                }
                if let Some(d) = v["data"].as_str() {
                    let _ = raw_in_tx.send(d.to_string()).await;
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Some(data) = raw_out_rx.recv().await {
            let msg = json!({"data": data}).to_string();
            if writer
                .send(tokio_tungstenite::tungstenite::Message::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Bound the handshake — a relay that accepts the socket but never
    // answers would otherwise hang the client forever.
    let e2e = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        E2e::client_handshake(&raw_out_tx, &mut raw_in_rx, token),
    )
    .await;
    let e2e = match e2e {
        Ok(Ok(e)) => e,
        r => {
            // Kill the reader pump: it owns the socket's read half, so
            // leaving it alive keeps the TCP connection (and the relay's
            // per-IP session slot) open after every failed attempt —
            // repeated bad-token redials would wedge the relay cap.
            read_pump.abort();
            match r {
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(anyhow::anyhow!("E2E handshake timed out")),
                Ok(Ok(_)) => unreachable!(),
            }
        }
    };
    let e2e = Arc::new(e2e);

    let (plain_in_tx, plain_in_rx) = mpsc::channel::<String>(64);
    let (plain_out_tx, plain_out_rx) = mpsc::channel::<String>(64);
    let e2e_in = e2e.clone();
    tokio::spawn(async move {
        while let Some(ct) = raw_in_rx.recv().await {
            match e2e_in.decrypt(&ct) {
                Ok(pt) => {
                    let _ = plain_in_tx.send(pt).await;
                }
                // Fail closed on tampered frames.
                Err(_) => return,
            }
        }
    });
    tokio::spawn(async move {
        let mut rx = plain_out_rx;
        while let Some(pt) = rx.recv().await {
            match e2e.encrypt(&pt) {
                Ok(ct) => {
                    let _ = raw_out_tx.send(ct).await;
                }
                // Oversized plaintext is refused by encrypt — surface it
                // instead of silently dropping the frame.
                Err(e) => warn!(error = %e, "E2E encrypt failed"),
            }
        }
    });
    Ok((plain_out_tx, plain_in_rx))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Percent-encode a query-param value (unreserved chars pass through).
fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Shared relay state for embedders/tests: daemon name → tunnel writer.
/// The `damon-relay` binary uses its own richer state (client ownership,
/// optional registration secret).
pub type RelayState = Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>;

pub fn new_relay_state() -> RelayState {
    Arc::new(Mutex::new(HashMap::new()))
}
