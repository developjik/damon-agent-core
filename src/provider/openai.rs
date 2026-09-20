use anyhow::{Context, bail};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde_json::Value;

use crate::config::ProviderCompat;
use crate::llm::StreamEvent;
use crate::provider::UpstreamResponse;

/// OpenAI chat-completions transport — passthrough with compat shaping.
pub struct OpenAiCompat {
    pub name: String,
    pub base_url: String,
    client: reqwest::Client,
    /// Static API key, or None when OAuth-backed.
    key: Option<String>,
    /// OAuth flavor ("kimi-code", "github-copilot", …) — resolve the
    /// credential from the keychain on each request instead of `key`.
    oauth_flavor: Option<String>,
    headers: Vec<(HeaderName, HeaderValue)>,
    compat: ProviderCompat,
}

/// Per-request OAuth credential: bearer token plus any flavor headers.
type OauthCred = Option<(String, Vec<(HeaderName, HeaderValue)>)>;

impl OpenAiCompat {
    pub fn new(
        name: &str,
        base_url: &str,
        key: Option<String>,
        headers: &std::collections::HashMap<String, String>,
        compat: ProviderCompat,
        oauth_flavor: Option<String>,
    ) -> anyhow::Result<Self> {
        if oauth_flavor.is_some() && key.is_some() {
            bail!("provider {name}: oauth and a static api_key are mutually exclusive");
        }

        // Static keys are validated up front — the key rides in an
        // Authorization header built per request in `credential()`.
        if let Some(k) = &key {
            HeaderValue::from_str(k)
                .context("provider api_key contains invalid header characters")?;
        }
        let headers = headers
            .iter()
            .map(|(k, v)| {
                Ok((
                    HeaderName::from_bytes(k.as_bytes())
                        .with_context(|| format!("invalid header name '{k}'"))?,
                    HeaderValue::from_str(v)
                        .with_context(|| format!("invalid header value for '{k}'"))?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            name: name.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            client: super::http_client(),
            key,
            oauth_flavor,
            headers,
            compat,
        })
    }

    /// (bearer token, extra per-flavor headers) — OAuth resolves live
    /// from the keychain (rotated tokens take effect without a restart).
    fn credential(&self) -> impl Future<Output = anyhow::Result<OauthCred>> + Send {
        let flavor = self.oauth_flavor.clone();
        async move {
            let Some(flavor) = flavor else {
                return Ok(self.key.clone().map(|k| (k, vec![])));
            };
            let c = crate::oauth::credential(&flavor).await?;
            let extra = match flavor.as_str() {
                // Headers the Copilot API expects from client integrations.
                "github-copilot" => vec![
                    (
                        HeaderName::from_static("editor-version"),
                        HeaderValue::from_static("copilot/1.0.82"),
                    ),
                    (
                        HeaderName::from_static("copilot-integration-id"),
                        HeaderValue::from_static("copilot-developer-cli"),
                    ),
                ],
                _ => vec![],
            };
            Ok(Some((c.access_token, extra)))
        }
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let req = match &self.key {
            Some(k) => req.header(AUTHORIZATION, format!("Bearer {k}")),
            None => req,
        };
        self.headers
            .iter()
            .fold(req, |r, (k, v)| r.header(k.clone(), v.clone()))
    }

    /// Mangle every tool name in the request body and return the
    /// mangled→original map for restoring response tool_calls. A map —
    /// not a textual reverse — because hash-shortened names don't invert.
    /// In-band tools render as text, so callers skip this entirely.
    fn mangle_tool_names(body: &mut Value) -> std::collections::HashMap<String, String> {
        // Collision detection needs every original name up front, so walk
        // the name sites read-only first.
        let mut originals = std::collections::HashSet::new();
        each_tool_name(body, &mut |n| {
            if let Some(s) = n.as_str() {
                originals.insert(s.to_string());
            }
        });
        let mut map = std::collections::HashMap::new();
        each_tool_name(body, &mut |n| mangle_name(n, &mut map, &originals));
        map
    }

    pub async fn forward(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Bytes,
    ) -> anyhow::Result<UpstreamResponse> {
        // Compat shaping applies to chat-completions payloads only.
        let shaped: Option<Value> =
            if method == reqwest::Method::POST && path == "/chat/completions" {
                serde_json::from_slice::<Value>(&body).ok().map(|mut v| {
                    if self.compat.inband_tools {
                        crate::provider::inband::render_tools(&mut v);
                    }
                    crate::provider::compat::apply(&mut v, &self.compat);
                    v
                })
            } else {
                None
            };
        let body = match &shaped {
            Some(v) => Bytes::from(v.to_string()),
            None => body,
        };
        // Azure OpenAI rides the deployment in the path with an
        // `api-version` query: POST /chat/completions becomes
        // `{base}/deployments/{model}/chat/completions?api-version=V`
        // (the model name in the body selects the deployment).
        let url = if self.compat.azure_deployment_urls {
            let version = self.compat.azure_api_version_or_default();
            if method == reqwest::Method::POST && path == "/chat/completions" {
                let model = shaped
                    .as_ref()
                    .and_then(|v| v["model"].as_str())
                    .unwrap_or_default();
                format!(
                    "{}/deployments/{model}{path}?api-version={version}",
                    self.base_url
                )
            } else {
                format!(
                    "{}/{}?api-version={version}",
                    self.base_url,
                    path.trim_start_matches('/')
                )
            }
        } else {
            format!("{}{}", self.base_url, path)
        };
        // OAuth: resolve the credential per request (rotated tokens take
        // effect without a restart); a 401 forces one refresh + retry.
        let cred = self.credential().await?;
        let send = {
            let method = method.clone();
            let url = url.clone();
            move |cred: Option<(String, Vec<(HeaderName, HeaderValue)>)>, body: Bytes| {
                let method = method.clone();
                let url = url.clone();
                async move {
                    let mut req = self.client.request(method, &url);
                    if let Some((bearer, extra)) = &cred {
                        req = req.header(AUTHORIZATION, format!("Bearer {bearer}"));
                        for (k, v) in extra {
                            req = req.header(k.clone(), v.clone());
                        }
                    }
                    self.authed(req)
                        .header("content-type", "application/json")
                        .body(body)
                        .send()
                        .await
                }
            }
        };
        let mut resp = send(cred.clone(), body.clone())
            .await
            .context("upstream request failed")?;
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.oauth_flavor.is_some() {
            let flavor = self.oauth_flavor.clone().unwrap_or_default();
            let c = crate::oauth::force_credential(&flavor).await?;
            resp = send(Some((c.access_token, vec![])), body.clone())
                .await
                .context("upstream request failed after token refresh")?;
        }

        // Strict-tools fallback: a 400 mentioning "strict" on a request that
        // carried strict tools retries once without the field.
        if resp.status() == reqwest::StatusCode::BAD_REQUEST
            && let Some(v) = &shaped
        {
            let has_strict = v["tools"]
                .as_array()
                .is_some_and(|tools| tools.iter().any(|t| t["strict"].as_bool() == Some(true)));
            if has_strict {
                let status = resp.status();
                // Capture before text() consumes the response — the rebuilt
                // error must carry the real content-type, not a fabricated
                // one (a text/plain error would be misclassified as JSON).
                let content_type = resp
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/json")
                    .to_string();
                let err = resp.text().await.unwrap_or_default();
                if err.contains("strict") {
                    let mut retry = v.clone();
                    if let Some(tools) = retry["tools"].as_array_mut() {
                        for t in tools.iter_mut() {
                            t.as_object_mut().map(|o| o.remove("strict"));
                        }
                    }
                    resp = send(cred.clone(), Bytes::from(retry.to_string()))
                        .await
                        .context("upstream request failed")?;
                } else {
                    // Rebuild a response carrying the buffered error body.
                    return Ok(UpstreamResponse {
                        status,
                        content_type,
                        retry_after: None,
                        stream: Box::pin(futures::stream::once(
                            async move { Ok(Bytes::from(err)) },
                        )),
                    });
                }
            }
        }

        Ok(UpstreamResponse {
            status: resp.status(),
            retry_after: crate::provider::retry_after_opt(&resp),
            content_type: resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string(),
            stream: Box::pin(
                resp.bytes_stream()
                    .map(|chunk| chunk.map_err(std::io::Error::other)),
            ),
        })
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>>
    {
        let mut body = body;
        body["stream"] = Value::Bool(true);
        // MCP names (`server.tool`) violate the OpenAI function-name spec
        // `^[a-zA-Z0-9_-]{1,64}$` — strict endpoints 400 on the dot.
        let names = if self.compat.inband_tools {
            std::collections::HashMap::new()
        } else {
            Self::mangle_tool_names(&mut body)
        };
        let up = self
            .forward(
                reqwest::Method::POST,
                "/chat/completions",
                Bytes::from(body.to_string()),
            )
            .await?;
        if up.status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Honor the upstream's Retry-After when it sent one; otherwise
            // a short fixed backoff — the caller retries once.
            let hint = up.retry_after.unwrap_or(std::time::Duration::from_secs(2));
            return Err(crate::provider::RateLimited(hint).into());
        }
        if !up.status.is_success() {
            let buf = crate::provider::collect_stream(up.stream).await?;
            bail!("upstream {}: {}", up.status, String::from_utf8_lossy(&buf));
        }
        // In-band tools: buffer the whole stream so <tool_call> blocks can be
        // extracted from the complete text before emitting events.
        if self.compat.inband_tools {
            let events: Vec<StreamEvent> = sse_events(up.stream)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<anyhow::Result<_>>()?;
            return Ok(Box::new(futures::stream::iter(
                crate::provider::inband::events_with_tool_calls(events)
                    .into_iter()
                    .map(Ok),
            )));
        }
        let stream = sse_events(up.stream);
        if names.is_empty() {
            return Ok(Box::new(stream));
        }
        // Restore original tool names in streamed ToolCallDeltas.
        Ok(Box::new(stream.map(move |res| {
            res.map(|ev| match ev {
                StreamEvent::ToolCallDelta {
                    index,
                    id,
                    name,
                    arguments,
                } => StreamEvent::ToolCallDelta {
                    index,
                    id,
                    name: name.map(|n| names.get(&n).cloned().unwrap_or(n)),
                    arguments,
                },
                ev => ev,
            })
        })))
    }

    pub async fn chat(&self, mut body: Value) -> anyhow::Result<Value> {
        body["stream"] = Value::Bool(false);
        let names = if self.compat.inband_tools {
            std::collections::HashMap::new()
        } else {
            Self::mangle_tool_names(&mut body)
        };
        let up = self
            .forward(
                reqwest::Method::POST,
                "/chat/completions",
                Bytes::from(body.to_string()),
            )
            .await?;
        if up.status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let hint = up.retry_after.unwrap_or(std::time::Duration::from_secs(2));
            return Err(crate::provider::RateLimited(hint).into());
        }
        let buf = crate::provider::collect_stream(up.stream).await?;
        if !up.status.is_success() {
            bail!("upstream {}: {}", up.status, String::from_utf8_lossy(&buf));
        }
        let mut v: Value = serde_json::from_slice(&buf)?;
        if self.compat.inband_tools {
            crate::provider::inband::response_with_tool_calls(&mut v);
        } else {
            unmangle_tool_calls(&mut v, &names);
        }
        Ok(v)
    }

    pub async fn list_models(&self) -> anyhow::Result<Value> {
        let up = self
            .forward(reqwest::Method::GET, "/models", Bytes::new())
            .await?;
        let buf = crate::provider::collect_stream(up.stream).await?;
        if !up.status.is_success() {
            bail!("upstream {}: {}", up.status, String::from_utf8_lossy(&buf));
        }
        Ok(serde_json::from_slice(&buf)?)
    }
}

/// Visit every tool-name site in a chat-completions body:
/// `tools[].function.name`, replayed `messages[].tool_calls[].function.name`
/// and an explicit `tool_choice.function.name`.
fn each_tool_name(body: &mut Value, f: &mut impl FnMut(&mut Value)) {
    if let Some(tools) = body.get_mut("tools").and_then(|t| t.as_array_mut()) {
        for t in tools {
            if let Some(n) = t.get_mut("function").and_then(|x| x.get_mut("name")) {
                f(n);
            }
        }
    }
    // Replayed assistant tool_calls and an explicit tool_choice carry
    // the same names — upstream validates them too.
    if let Some(msgs) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
        for m in msgs {
            if let Some(calls) = m.get_mut("tool_calls").and_then(|c| c.as_array_mut()) {
                for c in calls {
                    if let Some(n) = c.get_mut("function").and_then(|x| x.get_mut("name")) {
                        f(n);
                    }
                }
            }
        }
    }
    if let Some(tc) = body.get_mut("tool_choice")
        && tc["type"].as_str() == Some("function")
        && let Some(n) = tc.get_mut("function").and_then(|x| x.get_mut("name"))
    {
        f(n);
    }
}

/// Mangle one name value in place; record the mapping when it changed.
/// When the mangled form is claimed by a different original name — a
/// literal `server__tool` defined next to an MCP `server.tool` — the
/// wire name gets a `__<sha256[:8]>` variant instead, so both stay
/// distinct and every response name restores to its own original.
fn mangle_name(
    v: &mut Value,
    map: &mut std::collections::HashMap<String, String>,
    originals: &std::collections::HashSet<String>,
) {
    let Some(name) = v.as_str() else {
        return;
    };
    let name = name.to_string();
    let base = mangle(&name);
    let sent = if (base != name && originals.contains(&base))
        || map.get(&base).is_some_and(|orig| orig != &name)
    {
        variant_name(&base, &name, map, originals)
    } else {
        base
    };
    if sent != name {
        map.insert(sent.clone(), name);
        *v = Value::String(sent);
    }
}

/// A wire name unique across this request: `base` with a `__<sha256[:8]>`
/// suffix of `name`, re-hashed until neither another original nor an
/// already-assigned wire name claims it. Trimmed to the same 64-char cap
/// `mangle` enforces.
fn variant_name(
    base: &str,
    name: &str,
    map: &std::collections::HashMap<String, String>,
    originals: &std::collections::HashSet<String>,
) -> String {
    use sha2::Digest;
    let taken = |candidate: &str| {
        (originals.contains(candidate) && candidate != name)
            || map.get(candidate).is_some_and(|orig| orig != name)
    };
    let prefix: String = base.chars().take(54).collect();
    let mut digest = sha2::Sha256::digest(name.as_bytes());
    loop {
        let hash: String = digest[..4].iter().map(|b| format!("{b:02x}")).collect();
        let candidate = format!("{prefix}__{hash}");
        if !taken(&candidate) {
            return candidate;
        }
        digest = sha2::Sha256::digest(candidate.as_bytes());
    }
}

/// `server.tool` → `server__tool`; other characters outside
/// `[a-zA-Z0-9_-]` → `_`. Names over 64 chars get a `__<sha256[:8]>`
/// suffix so they still fit the OpenAI limit.
fn mangle(name: &str) -> String {
    let mut m = String::with_capacity(name.len());
    for c in name.chars() {
        match c {
            '.' => m.push_str("__"),
            c if c.is_ascii_alphanumeric() || c == '_' || c == '-' => m.push(c),
            _ => m.push('_'),
        }
    }
    if m.chars().count() > 64 {
        use sha2::Digest;
        let digest = sha2::Sha256::digest(name.as_bytes());
        let hash: String = digest[..4].iter().map(|b| format!("{b:02x}")).collect();
        let prefix: String = m.chars().take(54).collect();
        m = format!("{prefix}__{hash}");
    }
    m
}

/// Restore original tool names in a non-streaming chat-completions
/// response (`choices[].message.tool_calls[].function.name`).
fn unmangle_tool_calls(v: &mut Value, map: &std::collections::HashMap<String, String>) {
    let Some(choices) = v.get_mut("choices").and_then(|c| c.as_array_mut()) else {
        return;
    };
    for choice in choices {
        let Some(calls) = choice
            .get_mut("message")
            .and_then(|m| m.get_mut("tool_calls"))
            .and_then(|t| t.as_array_mut())
        else {
            continue;
        };
        for call in calls {
            if let Some(n) = call.get_mut("function").and_then(|f| f.get_mut("name"))
                && let Some(orig) = n.as_str().and_then(|s| map.get(s)).cloned()
            {
                *n = Value::String(orig);
            }
        }
    }
}

/// Parse an SSE byte stream into normalized StreamEvents. Shared by
/// adapters that emit OpenAI-shaped SSE (openai-compat) — others translate.
///
/// Bytes are buffered raw and decoded only at `\n` boundaries, so a
/// multi-byte UTF-8 char split across TCP chunks is never lossy-decoded.
/// `data:` lines accumulate until the blank-line event boundary (SSE
/// permits multi-line data, joined with `\n`).
pub fn sse_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    // finish carries the last-seen finish_reason into the terminal Done.
    // pending queues extra events from a multi-tool-call chunk.
    Box::pin(futures::stream::unfold(
        (
            stream,
            Vec::<u8>::new(),
            Vec::<String>::new(),
            std::collections::VecDeque::<anyhow::Result<StreamEvent>>::new(),
            false,
            crate::llm::StopReason::Other,
            // A terminal marker ([DONE] or a finish_reason) distinguishes a
            // completed stream from a truncated one — EOF without it is an
            // error, not a clean Done.
            false,
            Slots::default(),
        ),
        |(
            mut stream,
            mut buf,
            mut data_lines,
            mut pending,
            mut done,
            mut finish,
            mut saw_terminal,
            mut slots,
        )| async move {
            // Drain queued events from a multi-event chunk first.
            if let Some(res) = pending.pop_front() {
                if matches!(res, Ok(StreamEvent::Done(_))) {
                    done = true;
                }
                return Some((
                    res,
                    (
                        stream,
                        buf,
                        data_lines,
                        pending,
                        done,
                        finish,
                        saw_terminal,
                        slots,
                    ),
                ));
            }
            if done {
                return None;
            }
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    // buf[..pos] is a complete UTF-8 boundary: \n (0x0A)
                    // never appears inside a multi-byte sequence.
                    let line = String::from_utf8_lossy(&buf[..pos])
                        .trim_end_matches('\r')
                        .to_string();
                    buf.drain(..=pos);
                    if line.is_empty() {
                        match flush_event(
                            &mut data_lines,
                            &mut finish,
                            &mut saw_terminal,
                            &mut slots,
                        ) {
                            Some(Ok(evs)) => {
                                let mut it = evs.into_iter();
                                if let Some(first) = it.next() {
                                    for ev in it {
                                        pending.push_back(Ok(ev));
                                    }
                                    if matches!(first, StreamEvent::Done(_)) {
                                        done = true;
                                    }
                                    return Some((
                                        Ok(first),
                                        (
                                            stream,
                                            buf,
                                            data_lines,
                                            pending,
                                            done,
                                            finish,
                                            saw_terminal,
                                            slots,
                                        ),
                                    ));
                                }
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(e),
                                    (
                                        stream,
                                        buf,
                                        data_lines,
                                        pending,
                                        done,
                                        finish,
                                        saw_terminal,
                                        slots,
                                    ),
                                ));
                            }
                            None => {}
                        }
                        continue;
                    }
                    if let Some(data) = line.strip_prefix("data:") {
                        data_lines.push(data.trim().to_string());
                    }
                    continue;
                }
                match stream.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        done = true;
                        return Some((
                            Err(e.into()),
                            (
                                stream,
                                buf,
                                data_lines,
                                pending,
                                done,
                                finish,
                                saw_terminal,
                                slots,
                            ),
                        ));
                    }
                    None => {
                        // Flush a trailing partial line, then any pending
                        // event, then emit Done.
                        if !buf.is_empty() {
                            let line = String::from_utf8_lossy(&buf)
                                .trim_end_matches('\r')
                                .to_string();
                            buf.clear();
                            if let Some(data) = line.strip_prefix("data:") {
                                data_lines.push(data.trim().to_string());
                            }
                        }
                        match flush_event(
                            &mut data_lines,
                            &mut finish,
                            &mut saw_terminal,
                            &mut slots,
                        ) {
                            Some(Ok(evs)) => {
                                let mut it = evs.into_iter();
                                if let Some(first) = it.next() {
                                    for ev in it {
                                        pending.push_back(Ok(ev));
                                    }
                                    if matches!(first, StreamEvent::Done(_)) {
                                        done = true;
                                    }
                                    return Some((
                                        Ok(first),
                                        (
                                            stream,
                                            buf,
                                            data_lines,
                                            pending,
                                            done,
                                            finish,
                                            saw_terminal,
                                            slots,
                                        ),
                                    ));
                                }
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(e),
                                    (
                                        stream,
                                        buf,
                                        data_lines,
                                        pending,
                                        done,
                                        finish,
                                        saw_terminal,
                                        slots,
                                    ),
                                ));
                            }
                            None => {}
                        }
                        done = true;
                        // EOF without a terminal marker ([DONE] or a
                        // finish_reason) is a truncated stream — surface
                        // an error so the caller can retry, not a clean
                        // Done that persists partial text as finished.
                        if !saw_terminal {
                            return Some((
                                Err(anyhow::anyhow!("stream ended without terminal event")),
                                (
                                    stream,
                                    buf,
                                    data_lines,
                                    pending,
                                    done,
                                    finish,
                                    saw_terminal,
                                    slots,
                                ),
                            ));
                        }
                        return Some((
                            Ok(StreamEvent::Done(finish)),
                            (
                                stream,
                                buf,
                                data_lines,
                                pending,
                                done,
                                finish,
                                saw_terminal,
                                slots,
                            ),
                        ));
                    }
                }
            }
        },
    ))
}
/// Stream state for tool-call slot assignment. OpenAI keys parallel calls
/// by `index`, but some gateways (llama.cpp) omit it and identify calls by
/// `id` — without this map every delta would land in slot 0 and splice
/// parallel arguments together.
#[derive(Default)]
struct Slots {
    ids: std::collections::HashMap<String, usize>,
    next: usize,
}

impl Slots {
    fn slot(&mut self, call: &Value) -> usize {
        if let Some(i) = call["index"].as_u64() {
            let i = i as usize;
            if let Some(id) = call["id"].as_str() {
                self.ids.insert(id.to_string(), i);
            }
            self.next = self.next.max(i + 1);
            return i;
        }
        if let Some(id) = call["id"].as_str() {
            if !self.ids.contains_key(id) {
                self.ids.insert(id.to_string(), self.next);
                self.next += 1;
            }
            return self.ids[id];
        }
        0
    }
}

fn flush_event(
    data_lines: &mut Vec<String>,
    finish: &mut crate::llm::StopReason,
    saw_terminal: &mut bool,
    slots: &mut Slots,
) -> Option<anyhow::Result<Vec<StreamEvent>>> {
    let data = data_lines.join("\n");
    data_lines.clear();
    if data.is_empty() {
        return None;
    }
    if data == "[DONE]" {
        *saw_terminal = true;
        return Some(Ok(vec![StreamEvent::Done(*finish)]));
    }
    match parse_chunk(&data, slots) {
        Ok(parsed) => emit_parsed(parsed, finish, saw_terminal),
        Err(e) => {
            // SSE joins multi-line data with \n — one JSON value survives
            // the join (\n is JSON whitespace), but a non-typical upstream
            // can pack two complete values into one event. Re-parse line
            // by line: keep what parses, log and skip the rest instead of
            // failing the whole stream. All lines failing keeps the join
            // error — that is genuinely corrupt data.
            let mut merged = Parsed::default();
            let mut any = false;
            for line in data.split('\n') {
                if line.is_empty() {
                    continue;
                }
                match parse_chunk(line, slots) {
                    Ok(p) => {
                        any = true;
                        if let Some(r) = p.finish {
                            merged.finish = Some(r);
                        }
                        merged.events.extend(p.events);
                    }
                    Err(_) => tracing::debug!("skipping unparsable SSE data line: {line:?}"),
                }
            }
            if !any {
                return Some(Err(e));
            }
            emit_parsed(merged, finish, saw_terminal)
        }
    }
}

/// Apply a successfully parsed chunk to the stream state.
fn emit_parsed(
    parsed: Parsed,
    finish: &mut crate::llm::StopReason,
    saw_terminal: &mut bool,
) -> Option<anyhow::Result<Vec<StreamEvent>>> {
    if let Some(r) = parsed.finish {
        *finish = r;
        *saw_terminal = true;
    }
    if parsed.events.is_empty() {
        None
    } else {
        Some(Ok(parsed.events))
    }
}

/// What a parsed SSE chunk produced: events plus an optional
/// finish_reason update (a chunk can carry both usage and a finish).
#[derive(Default)]
struct Parsed {
    events: Vec<StreamEvent>,
    finish: Option<crate::llm::StopReason>,
}

fn parse_chunk(data: &str, slots: &mut Slots) -> anyhow::Result<Parsed> {
    use crate::llm::StopReason;
    let v: Value = serde_json::from_str(data).context("invalid SSE JSON")?;
    let mut out = Parsed::default();

    // Usage may ride on the same chunk as content/finish — collect it
    // without dropping the rest.
    if let Some(u) = v.get("usage").filter(|u| u.is_object()) {
        out.events.push(StreamEvent::Usage {
            input: u["prompt_tokens"].as_u64().unwrap_or(0),
            output: u["completion_tokens"].as_u64().unwrap_or(0),
        });
    }

    let Some(choice) = v["choices"].get(0) else {
        return Ok(out);
    };
    let delta = &choice["delta"];

    // Reasoning deltas (DeepSeek-R1, OpenRouter, z.ai).
    for field in ["reasoning_content", "reasoning", "reasoning_text"] {
        if let Some(t) = delta[field].as_str()
            && !t.is_empty()
        {
            out.events.push(StreamEvent::Thinking(t.to_string()));
        }
    }

    if let Some(text) = delta["content"].as_str()
        && !text.is_empty()
    {
        out.events.push(StreamEvent::Text(text.to_string()));
    }
    if let Some(calls) = delta["tool_calls"].as_array() {
        for call in calls {
            let index = slots.slot(call);
            let f = &call["function"];
            out.events.push(StreamEvent::ToolCallDelta {
                index,
                id: call["id"].as_str().map(String::from),
                name: f["name"].as_str().map(String::from),
                arguments: f["arguments"].as_str().unwrap_or("").to_string(),
            });
        }
    }

    // finish_reason rides on the last content chunk — possibly alongside
    // usage and trailing deltas.
    if let Some(reason) = choice["finish_reason"].as_str() {
        out.finish = Some(match reason {
            "stop" => StopReason::Stop,
            "length" | "max_tokens" => StopReason::Length,
            "tool_calls" | "function_call" => StopReason::ToolCalls,
            _ => StopReason::Other,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A literal tool whose name equals another tool's mangled form must
    /// keep working: the colliding pair gets distinct wire names and each
    /// response name restores to its own original.
    #[test]
    fn mangle_collision_keeps_both_tools_distinct() {
        let mut body = json!({
            "tools": [
                {"type": "function", "function": {"name": "fs.read", "parameters": {}}},
                {"type": "function", "function": {"name": "fs__read", "parameters": {}}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "fs__read"}},
            "messages": [
                {"role": "assistant", "content": null, "tool_calls": [{
                    "id": "call_x", "type": "function",
                    "function": {"name": "fs.read", "arguments": "{}"}
                }]},
            ],
        });
        let names = OpenAiCompat::mangle_tool_names(&mut body);

        let wire_read = body["tools"][0]["function"]["name"].as_str().unwrap();
        let wire_literal = body["tools"][1]["function"]["name"].as_str().unwrap();
        assert_ne!(
            wire_read, wire_literal,
            "colliding tools must diverge on the wire"
        );
        assert_eq!(wire_literal, "fs__read");
        // Every site carrying the same original shares one wire name.
        assert_eq!(
            body["messages"][0]["tool_calls"][0]["function"]["name"],
            wire_read
        );
        assert_eq!(body["tool_choice"]["function"]["name"], wire_literal);
        for w in [wire_read, wire_literal] {
            assert!(w.chars().count() <= 64);
            assert!(
                w.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            );
        }

        // Both directions restore to the right original.
        let mut resp = json!({"choices": [{"message": {"tool_calls": [
            {"function": {"name": wire_read, "arguments": "{}"}},
            {"function": {"name": wire_literal, "arguments": "{}"}},
        ]}}]});
        unmangle_tool_calls(&mut resp, &names);
        let restored: Vec<&str> = resp["choices"][0]["message"]["tool_calls"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["function"]["name"].as_str().unwrap())
            .collect();
        assert!(restored.contains(&"fs.read"), "got {restored:?}");
        assert!(restored.contains(&"fs__read"), "got {restored:?}");
    }

    /// Gateways that omit `index` and identify calls by `id` (llama.cpp)
    /// must not have every delta collapse into slot 0.
    #[test]
    fn parse_chunk_keys_slots_by_id_when_index_absent() {
        let mut slots = Slots::default();
        let chunk = |id: &str, args: &str| {
            format!(
                r#"{{"choices":[{{"delta":{{"tool_calls":[{{"id":"{id}","function":{{"arguments":"{args}"}}}}]}}}}]}}"#
            )
        };
        let idx = |p: &Parsed| match &p.events[0] {
            StreamEvent::ToolCallDelta { index, .. } => *index,
            other => panic!("expected tool call delta, got {other:?}"),
        };

        let a1 = parse_chunk(&chunk("call_a", "{"), &mut slots).unwrap();
        let b1 = parse_chunk(&chunk("call_b", "["), &mut slots).unwrap();
        let a2 = parse_chunk(&chunk("call_a", "}"), &mut slots).unwrap();
        assert_eq!(idx(&a1), 0);
        assert_eq!(idx(&b1), 1);
        assert_eq!(idx(&a2), 0, "same id returns to its own slot");

        // Explicit index still wins — normal streams are untouched.
        let with_index = parse_chunk(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":5,"function":{"arguments":"x"}}]}}]}"#,
            &mut slots,
        )
        .unwrap();
        assert_eq!(idx(&with_index), 5);
    }

    /// Two JSON values packed into one multi-line SSE event must not fail
    /// the whole stream — line-wise reparse keeps both.
    #[test]
    fn flush_event_keeps_two_json_values_in_one_event() {
        let mut lines = vec![
            r#"{"choices":[{"delta":{"content":"a"}}]}"#.to_string(),
            r#"{"choices":[{"delta":{"content":"b"}}]}"#.to_string(),
        ];
        let mut finish = crate::llm::StopReason::Other;
        let mut saw_terminal = false;
        let mut slots = Slots::default();
        let evs = flush_event(&mut lines, &mut finish, &mut saw_terminal, &mut slots)
            .unwrap()
            .unwrap();
        assert_eq!(evs.len(), 2);
        assert!(matches!(&evs[0], StreamEvent::Text(t) if t == "a"));
        assert!(matches!(&evs[1], StreamEvent::Text(t) if t == "b"));
    }
}
