//! Outbound HTTP send helper shared by the channel adapters: retries
//! 429s honoring the peer's advertised wait, and paces the next send
//! when a Discord-style rate-limit bucket is exhausted. Previously each
//! adapter retried a 429 exactly once — a second consecutive 429 failed
//! the whole turn.

use std::time::Duration;

/// Total attempts per send — one initial try plus up to 3 rate-limit
/// retries. Only a peer still limiting after this many honored waits
/// fails the send.
const MAX_ATTEMPTS: usize = 4;

/// Longest single wait honored from a peer — a hostile or buggy
/// rate-limit header must not park a turn indefinitely.
const MAX_WAIT: Duration = Duration::from_secs(30);

/// Floor for any honored wait — a 0-second Retry-After still yields a
/// real gap between attempts.
const MIN_WAIT: Duration = Duration::from_millis(250);

/// Cap on the post-success pacing wait when a bucket is exhausted
/// (`X-RateLimit-Remaining: 0`) — keeps long multi-chunk replies from
/// tripping one 429 per chunk without stalling the turn on long
/// windows (the retry loop covers those).
const MAX_PACE: Duration = Duration::from_secs(15);

/// Send with rate-limit handling. `send` is (re)invoked per attempt and
/// must rebuild the request — a `reqwest::Response` body is single-use.
///
/// On 429 the wait comes from the `Retry-After` header (Slack, Discord),
/// else the JSON body — `retry_after` (Discord) or
/// `parameters.retry_after` (Telegram) — else exponential 1s/2s/4s
/// backoff. All waits are clamped to [250ms, 30s].
///
/// After a successful (or final) response carrying
/// `X-RateLimit-Remaining: 0`, sleeps up to `X-RateLimit-Reset-After`
/// (clamped) first, so the caller's NEXT send starts with a fresh
/// bucket instead of burning an attempt on a certain 429.
pub async fn send_with_rate_limit<F, Fut>(mut send: F) -> reqwest::Result<reqwest::Response>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = reqwest::Result<reqwest::Response>>,
{
    let mut attempt = 1;
    loop {
        let resp = send().await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS && attempt < MAX_ATTEMPTS {
            attempt += 1;
            let fallback = Duration::from_secs(1 << (attempt - 2).min(4));
            let wait = match retry_after_header(&resp) {
                Some(w) => w,
                None => retry_after_body(resp).await.unwrap_or(fallback),
            };
            tokio::time::sleep(wait.clamp(MIN_WAIT, MAX_WAIT)).await;
            continue;
        }
        pace_if_exhausted(&resp).await;
        return Ok(resp);
    }
}

/// `Retry-After` header as a duration. These APIs all use numeric
/// seconds (never HTTP-dates); anything unparseable is None.
fn retry_after_header(resp: &reqwest::Response) -> Option<Duration> {
    let v = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    let secs = v.trim().parse::<f64>().ok()?;
    secs.is_finite().then(|| Duration::from_secs_f64(secs))
}

/// Advertised wait inside a 429 body: Discord's top-level
/// `retry_after` or Telegram's `parameters.retry_after`. Consumes the
/// body — only called on responses we are about to discard and retry.
async fn retry_after_body(resp: reqwest::Response) -> Option<Duration> {
    let body = resp.text().await.ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    let secs = v["retry_after"]
        .as_f64()
        .or_else(|| v["parameters"]["retry_after"].as_f64())?;
    secs.is_finite().then(|| Duration::from_secs_f64(secs))
}

/// Post-success pacing: when the response says its bucket is empty
/// (`X-RateLimit-Remaining: 0`), wait out the reset window (clamped)
/// before the caller's next send. Headers only — the body stays
/// unread for the caller.
async fn pace_if_exhausted(resp: &reqwest::Response) {
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<f64>().ok())
    };
    if header("x-ratelimit-remaining") != Some(0.0) {
        return;
    }
    let reset = header("x-ratelimit-reset-after").unwrap_or(1.0);
    tokio::time::sleep(Duration::from_secs_f64(reset).min(MAX_PACE)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16, headers: &[(&str, &str)], body: &str) -> reqwest::Response {
        let mut builder = axum::http::Response::builder().status(status);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        reqwest::Response::from(builder.body(body.to_string()).unwrap())
    }

    #[test]
    fn retry_after_header_parses_seconds() {
        let resp = response(429, &[("retry-after", "2.5")], "");
        assert_eq!(
            retry_after_header(&resp),
            Some(Duration::from_secs_f64(2.5))
        );
        let resp = response(429, &[("retry-after", "soon")], "");
        assert_eq!(retry_after_header(&resp), None);
    }

    #[tokio::test]
    async fn retry_after_body_covers_discord_and_telegram_shapes() {
        let discord = response(429, &[], r#"{"retry_after": 3.2, "global": false}"#);
        assert_eq!(
            retry_after_body(discord).await,
            Some(Duration::from_secs_f64(3.2))
        );
        let telegram = response(
            429,
            &[],
            r#"{"ok": false, "parameters": {"retry_after": 7}}"#,
        );
        assert_eq!(
            retry_after_body(telegram).await,
            Some(Duration::from_secs(7))
        );
        let junk = response(429, &[], "not json");
        assert_eq!(retry_after_body(junk).await, None);
    }

    #[tokio::test]
    async fn retries_until_non_429_then_returns_it() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let resp = send_with_rate_limit(|| {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if n < 2 {
                    Ok(response(429, &[("retry-after", "0")], ""))
                } else {
                    Ok(response(
                        200,
                        &[("content-type", "application/json")],
                        r#"{"ok": true}"#,
                    ))
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let resp = send_with_rate_limit(|| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { Ok(response(429, &[("retry-after", "0")], "")) }
        })
        .await
        .unwrap();
        assert_eq!(resp.status(), 429);
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            MAX_ATTEMPTS
        );
    }
}
