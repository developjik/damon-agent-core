//! SessionManager — maps Damon session ids to live backend sessions.
//!
//! One Damon session owns one `AgentSession` (which owns its process).
//! The manager routes prompts, permission answers, and interrupts, and
//! reaps idle sessions so unused agent processes don't pile up —
//! reattach happens through the persistence handle on next use.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::Mutex;

use crate::backend::types::*;
use crate::backend::{AgentClient, AgentSession};

/// A live Damon session: the backend session plus its routing metadata.
pub struct ManagedSession {
    /// Damon-facing session id.
    pub id: String,
    /// The backend that owns this session.
    pub provider: ProviderId,
    /// The live backend session object.
    pub session: Arc<dyn AgentSession>,
    /// Latest persistence handle (native session id), refreshed on
    /// ThreadStarted events.
    pub handle: Option<PersistenceHandle>,
    /// Working directory the session runs in.
    pub cwd: std::path::PathBuf,
    /// A turn is in flight — the idle sweep must not reap this session.
    pub busy: std::sync::atomic::AtomicBool,
}

/// Registry of backend clients keyed by provider id.
pub struct SessionManager {
    clients: parking_lot::RwLock<HashMap<ProviderId, Arc<dyn AgentClient>>>,
    sessions: Mutex<HashMap<String, Arc<ManagedSession>>>,
    /// Default provider when session.create omits `backend`.
    default_provider: parking_lot::RwLock<Option<String>>,
}

impl SessionManager {
    /// Build from daemon config: resolve every catalog backend against
    /// `[backends]` overrides, keep the detected ones, and wrap each in
    /// its client. Undetected backends are absent from the map — a
    /// session.create naming one fails with "not available".
    pub fn from_config(cfg: &crate::config::Config) -> Self {
        let resolved = crate::backend::registry::resolve_backends(&cfg.backends);
        let mut clients = HashMap::new();
        for r in resolved {
            if !r.detected {
                continue;
            }
            if let Some(client) = crate::backend::registry::client_for(&r) {
                clients.insert(r.id.clone(), client);
            }
        }
        Self::new(clients, cfg.default_backend.clone())
    }

    pub fn new(
        clients: HashMap<ProviderId, Arc<dyn AgentClient>>,
        default_provider: Option<String>,
    ) -> Self {
        Self {
            clients: parking_lot::RwLock::new(clients),
            sessions: Mutex::new(HashMap::new()),
            default_provider: parking_lot::RwLock::new(default_provider),
        }
    }
    /// Register an extra backend client — tests inject in-process mocks
    /// this way instead of spawning a real agent CLI.
    #[doc(hidden)]
    pub fn insert_client(&self, id: ProviderId, client: Arc<dyn AgentClient>) {
        self.clients.write().insert(id, client);
    }

    /// Rebuild the client map after a config reload. Live sessions keep
    /// their existing backend objects; only new creates/resumes see the
    /// refreshed clients.
    pub fn refresh(&self, cfg: &crate::config::Config) {
        let resolved = crate::backend::registry::resolve_backends(&cfg.backends);
        let mut clients = HashMap::new();
        for r in resolved {
            if !r.detected {
                continue;
            }
            if let Some(client) = crate::backend::registry::client_for(&r) {
                clients.insert(r.id.clone(), client);
            }
        }
        *self.clients.write() = clients;
        *self.default_provider.write() = cfg.default_backend.clone();
    }

    /// All registered (provider id, client) pairs — for backend.list.
    pub fn clients(&self) -> Vec<(ProviderId, Arc<dyn AgentClient>)> {
        self.clients
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Session ids with a turn in flight — retention/idle sweeps skip them.
    pub async fn busy_ids(&self) -> std::collections::HashSet<String> {
        self.sessions
            .lock()
            .await
            .iter()
            .filter(|(_, m)| m.busy.load(std::sync::atomic::Ordering::Relaxed))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Provider ids with a detected CLI.
    pub fn available_backends(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.clients.read().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Resolve which backend a new session uses: explicit `backend`
    /// param, the configured default, or the first available.
    fn resolve_provider(&self, requested: Option<&str>) -> Result<ProviderId> {
        let clients = self.clients.read();
        if let Some(id) = requested {
            if clients.contains_key(id) {
                return Ok(id.to_string());
            }
            bail!("backend `{id}` is not available");
        }
        if let Some(d) = self.default_provider.read().as_ref()
            && clients.contains_key(d)
        {
            return Ok(d.clone());
        }
        clients
            .keys()
            .next()
            .cloned()
            .context("no backends available — install claude, codex, or omp")
    }

    /// Create a session on the resolved backend.
    pub async fn create(
        &self,
        backend: Option<&str>,
        config: SessionConfig,
    ) -> Result<Arc<ManagedSession>> {
        let provider = self.resolve_provider(backend)?;
        let client = self.clients.read()[&provider].clone();
        let session = client.create_session(config.clone()).await?;
        let id = uuid::Uuid::new_v4().to_string();
        let managed = Arc::new(ManagedSession {
            id: id.clone(),
            provider,
            handle: session.persistence_handle(),
            session,
            cwd: config.cwd,
            busy: std::sync::atomic::AtomicBool::new(false),
        });
        self.sessions
            .lock()
            .await
            .insert(id.clone(), managed.clone());
        Ok(managed)
    }

    /// Resume a persisted session on its recorded backend.
    pub async fn resume(
        &self,
        session_id: &str,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<ManagedSession>> {
        let client = self
            .clients
            .read()
            .get(&handle.provider)
            .cloned()
            .with_context(|| format!("backend `{}` not available", handle.provider))?;
        let session = client
            .resume_session(handle, config.clone(), ResumePurpose::Interactive)
            .await?;
        let managed = Arc::new(ManagedSession {
            id: session_id.to_string(),
            provider: handle.provider.clone(),
            handle: Some(handle.clone()),
            session,
            cwd: config.cwd,
            busy: std::sync::atomic::AtomicBool::new(false),
        });
        self.sessions
            .lock()
            .await
            .insert(session_id.to_string(), managed.clone());
        Ok(managed)
    }

    /// Look up a live session.
    pub async fn get(&self, session_id: &str) -> Option<Arc<ManagedSession>> {
        self.sessions.lock().await.get(session_id).cloned()
    }

    /// Remove a session from the live map (does not close it).
    pub async fn detach(&self, session_id: &str) -> Option<Arc<ManagedSession>> {
        self.sessions.lock().await.remove(session_id)
    }

    /// Close and remove a session.
    pub async fn close(&self, session_id: &str) -> Result<()> {
        if let Some(m) = self.sessions.lock().await.remove(session_id) {
            m.session.close().await?;
        }
        Ok(())
    }

    /// Kill sessions idle longer than `max_idle`, skipping any with a
    /// live turn. Returns how long until the next sweep is worthwhile.
    pub async fn reap_idle(&self, max_idle: Duration) -> Duration {
        let mut nearest = max_idle;
        let mut to_close: Vec<String> = vec![];
        {
            let sessions = self.sessions.lock().await;
            for (id, m) in sessions.iter() {
                if m.busy.load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                let idle = Duration::from_secs(m.session.idle_secs());
                if idle >= max_idle {
                    to_close.push(id.clone());
                } else {
                    nearest = nearest.min(max_idle - idle);
                }
            }
        }
        for id in to_close {
            let _ = self.close(&id).await;
        }
        nearest.max(Duration::from_secs(1))
    }

    /// Close every session — daemon shutdown.
    pub async fn shutdown(&self) {
        let sessions: Vec<Arc<ManagedSession>> =
            self.sessions.lock().await.values().cloned().collect();
        for m in sessions {
            let _ = m.session.close().await;
        }
        self.sessions.lock().await.clear();
        let clients: Vec<Arc<dyn AgentClient>> = self.clients.read().values().cloned().collect();
        for client in clients {
            let _ = client.shutdown().await;
        }
    }
}
