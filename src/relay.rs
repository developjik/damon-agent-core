//! Remote relay: a public `damon-relay` server pipes WebSocket frames between
//! a registered daemon and connecting clients. The daemon dials OUT to the
//! relay (no inbound port needed); clients dial the relay by daemon name.
//!
//! E2E: the daemon and client do an X25519 handshake over the relay, each
//! proving knowledge of the shared `auth_token` via sha256(token || pubkey).
//! All JSON-RPC frames after the handshake are AES-256-GCM encrypted — the
//! relay sees only opaque ciphertext.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::Digest;
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};

use crate::api::AppState;

// ---------------------------------------------------------------------------
// E2E crypto
// ---------------------------------------------------------------------------

/// An established E2E session: encrypt/decrypt JSON text frames.
pub struct E2e {
    cipher: aes_gcm::Aes256Gcm,
}

impl E2e {
    /// Daemon side: generate a keypair, send {pub, proof} to the client,
    /// verify the client's {pub, proof}, derive the shared key.
    pub async fn daemon_handshake(
        tx: &mpsc::Sender<String>,
        rx: &mut mpsc::Receiver<String>,
        token: &str,
    ) -> anyhow::Result<Self> {
        let secret = x25519_dalek::StaticSecret::random_from_rng(getrandom_rng());
        let public = x25519_dalek::PublicKey::from(&secret);
        let my_proof = proof(token, public.as_bytes());
        tx.send(json!({"e2e_pub": B64.encode(public.as_bytes()), "e2e_proof": my_proof}).to_string())
            .await
            .context("send handshake")?;

        let msg = rx.recv().await.context("client handshake missing")?;
        let v: Value = serde_json::from_str(&msg)?;
        let their_pub = B64
            .decode(v["e2e_pub"].as_str().context("missing e2e_pub")?)
            .context("bad e2e_pub")?;
        let their_proof = v["e2e_proof"].as_str().context("missing e2e_proof")?;
        let their_pub: [u8; 32] = their_pub.try_into().map_err(|_| anyhow::anyhow!("bad pubkey len"))?;
        if their_proof != proof(token, &their_pub) {
            bail!("client failed E2E proof — wrong auth_token?");
        }
        let shared = secret.diffie_hellman(&x25519_dalek::PublicKey::from(their_pub));
        Ok(Self::from_shared(shared.as_bytes()))
    }

    /// Client side: receive the daemon's {pub, proof}, verify, send ours.
    pub async fn client_handshake(
        tx: &mpsc::Sender<String>,
        rx: &mut mpsc::Receiver<String>,
        token: &str,
    ) -> anyhow::Result<Self> {
        let msg = rx.recv().await.context("daemon handshake missing")?;
        let v: Value = serde_json::from_str(&msg)?;
        let their_pub = B64
            .decode(v["e2e_pub"].as_str().context("missing e2e_pub")?)
            .context("bad e2e_pub")?;
        let their_proof = v["e2e_proof"].as_str().context("missing e2e_proof")?;
        let their_pub: [u8; 32] = their_pub.try_into().map_err(|_| anyhow::anyhow!("bad pubkey len"))?;
        if their_proof != proof(token, &their_pub) {
            bail!("daemon failed E2E proof — wrong auth_token?");
        }
        let secret = x25519_dalek::StaticSecret::random_from_rng(getrandom_rng());
        let public = x25519_dalek::PublicKey::from(&secret);
        let my_proof = proof(token, public.as_bytes());
        tx.send(json!({"e2e_pub": B64.encode(public.as_bytes()), "e2e_proof": my_proof}).to_string())
            .await
            .context("send handshake")?;
        let shared = secret.diffie_hellman(&x25519_dalek::PublicKey::from(their_pub));
        Ok(Self::from_shared(shared.as_bytes()))
    }

    fn from_shared(shared: &[u8; 32]) -> Self {
        use aes_gcm::KeyInit;
        Self {
            cipher: aes_gcm::Aes256Gcm::new_from_slice(shared).expect("32-byte key"),
        }
    }

    /// Encrypt a JSON text frame → base64 ciphertext.
    pub fn encrypt(&self, plaintext: &str) -> anyhow::Result<String> {
        use aes_gcm::aead::Aead;
        let mut nonce = [0u8; 12];
        getrandom::fill(&mut nonce).expect("OS RNG");
        let ct = self
            .cipher
            .encrypt(aes_gcm::Nonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| anyhow::anyhow!("encrypt failed"))?;
        let mut frame = nonce.to_vec();
        frame.extend_from_slice(&ct);
        Ok(B64.encode(frame))
    }

    /// Decrypt a base64 ciphertext → JSON text.
    pub fn decrypt(&self, b64: &str) -> anyhow::Result<String> {
        use aes_gcm::aead::Aead;
        let frame = B64.decode(b64).context("bad base64")?;
        if frame.len() < 12 {
            bail!("frame too short");
        }
        let (nonce, ct) = frame.split_at(12);
        let pt = self
            .cipher
            .decrypt(aes_gcm::Nonce::from_slice(nonce), ct)
            .map_err(|_| anyhow::anyhow!("decrypt failed — wrong key or tampered"))?;
        Ok(String::from_utf8(pt)?)
    }
}

fn proof(token: &str, pubkey: &[u8]) -> String {
    let mut h = sha2::Sha256::new();
    h.update(token.as_bytes());
    h.update(pubkey);
    B64.encode(h.finalize())
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
            getrandom::fill(dest).map_err(|_| {
                rand_core::Error::from(std::num::NonZeroU32::MIN)
            })
        }
    }
    impl rand_core::CryptoRng for R {}
    R
}

// ---------------------------------------------------------------------------
// Daemon side: outbound tunnel to the relay
// ---------------------------------------------------------------------------

/// Connect to `relay_url` (ws://host:port), register as `name`, and serve
/// each client that connects through the relay. Reconnects on drop.
pub async fn run_tunnel(state: Arc<AppState>, relay_url: String, name: String) {
    loop {
        match tunnel_once(&state, &relay_url, &name).await {
            Ok(()) => info!("relay tunnel closed; reconnecting"),
            Err(e) => warn!(error = %e, "relay tunnel failed; reconnecting in 5s"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn tunnel_once(state: &Arc<AppState>, relay_url: &str, name: &str) -> anyhow::Result<()> {
    let url = format!("{}/register?name={}", relay_url.trim_end_matches('/'), name);
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .context("cannot reach relay")?;
    info!(relay = %relay_url, name = %name, "registered with relay");
    let (mut writer, mut reader) = ws.split();

    // The relay sends {"client": <id>} when a client connects, then pipes
    // that client's frames as {"client": id, "data": "..."}.
    // We send {"client": id, "data": "..."} back.
    let mut sessions: HashMap<u64, mpsc::Sender<String>> = HashMap::new();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);

    loop {
        tokio::select! {
            msg = reader.next() => {
                let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) = msg else {
                    if matches!(msg, Some(Err(_)) | None) { break; }
                    continue;
                };
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let client_id = v["client"].as_u64().unwrap_or(0);
                if let Some(data) = v["data"].as_str() {
                    // Frame from a client.
                    if let Some(in_tx) = sessions.get(&client_id) {
                        let _ = in_tx.send(data.to_string()).await;
                    }
                } else if v["client"].is_u64() && v.get("data").is_none() {
                    // New client connected — spawn a session.
                    let (in_tx, in_rx) = mpsc::channel::<String>(64);
                    let (sess_out, mut sess_rx) = mpsc::channel::<String>(64);
                    sessions.insert(client_id, in_tx);
                    let state = state.clone();
                    let out_tx = out_tx.clone();
                    // Pipe sess_out → relay (handshake + encrypted frames).
                    let out_tx2 = out_tx.clone();
                    tokio::spawn(async move {
                        while let Some(data) = sess_rx.recv().await {
                            let _ = out_tx2
                                .send(json!({"client": client_id, "data": data}).to_string())
                                .await;
                        }
                    });
                    tokio::spawn(async move {
                        // E2E handshake over the relay pipe.
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
                        let e2e = match E2e::daemon_handshake(&sess_out, &mut in_rx, &token).await {
                            Ok(e) => e,
                            Err(e) => {
                                warn!(error = %e, "E2E handshake failed");
                                return;
                            }
                        };
                        // Bridge: decrypt inbound, run handle_socket, encrypt outbound.
                        let (plain_in_tx, plain_in_rx) = mpsc::channel::<String>(64);
                        let (plain_out_tx, mut plain_out_rx) = mpsc::channel::<String>(64);
                        let e2e = Arc::new(e2e);
                        let e2e_in = e2e.clone();
                        tokio::spawn(async move {
                            while let Some(ct) = in_rx.recv().await {
                                match e2e_in.decrypt(&ct) {
                                    Ok(pt) => { let _ = plain_in_tx.send(pt).await; }
                                    Err(e) => warn!(error = %e, "E2E decrypt failed"),
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
                }
            }
            out = out_rx.recv() => {
                let Some(text) = out else { continue };
                if writer.send(tokio_tungstenite::tungstenite::Message::Text(text.into())).await.is_err() {
                    break;
                }
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
    let url = format!("{}/connect?name={}", relay_url.trim_end_matches('/'), name);
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .context("cannot reach relay")?;
    let (mut writer, mut reader) = ws.split();

    let (raw_in_tx, mut raw_in_rx) = mpsc::channel::<String>(64);
    let (raw_out_tx, mut raw_out_rx) = mpsc::channel::<String>(64);
    tokio::spawn(async move {
        while let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) = reader.next().await {
            if let Ok(v) = serde_json::from_str::<Value>(&t) {
                if let Some(d) = v["data"].as_str() {
                    let _ = raw_in_tx.send(d.to_string()).await;
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Some(data) = raw_out_rx.recv().await {
            let msg = json!({"data": data}).to_string();
            if writer.send(tokio_tungstenite::tungstenite::Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let e2e = E2e::client_handshake(&raw_out_tx, &mut raw_in_rx, token).await?;
    let e2e = Arc::new(e2e);

    let (plain_in_tx, plain_in_rx) = mpsc::channel::<String>(64);
    let (plain_out_tx, plain_out_rx) = mpsc::channel::<String>(64);
    let e2e_in = e2e.clone();
    tokio::spawn(async move {
        while let Some(ct) = raw_in_rx.recv().await {
            if let Ok(pt) = e2e_in.decrypt(&ct) {
                let _ = plain_in_tx.send(pt).await;
            }
        }
    });
    tokio::spawn(async move {
        let mut rx = plain_out_rx;
        while let Some(pt) = rx.recv().await {
            if let Ok(ct) = e2e.encrypt(&pt) {
                let _ = raw_out_tx.send(ct).await;
            }
        }
    });
    Ok((plain_out_tx, plain_in_rx))
}

// ---------------------------------------------------------------------------
// Relay server binary logic (used by src/bin/damon-relay.rs)
// ---------------------------------------------------------------------------

/// Shared relay state: daemon name → its outbound-tunnel writer.
pub type RelayState = Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>;

pub fn new_relay_state() -> RelayState {
    Arc::new(Mutex::new(HashMap::new()))
}
