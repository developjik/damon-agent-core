//! `permission.respond` — the answer half of a turn's ask (see
//! [`super::params`] for the typed-params machinery this family follows).
//!
//! It is its own family because its shape is its own: the request names a
//! session, an ask, and the response object the backend eats — no
//! `prompt`, no prompt payload. A plan ask's decision also lands in the
//! durable review row here (intent before the native write), which is why
//! `plan.*` and this method meet in `crate::store` rather than here.
//!
//! Wire behavior is unchanged on purpose — codes, messages and the
//! silently-ignored optional params included.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::AppState;
use crate::backend::types::PermissionResponse;

use super::params::{Field, Params};
use super::{RpcError, error_code};

/// `permission.respond` — allow or deny one pending ask.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Respond {
    session_id: String,
    request_id: String,
    /// `{behavior: allow|deny, …}`. Kept as the raw JSON because
    /// deserializing it is the check: the legacy handler answered
    /// `response required` for anything that was not a
    /// [`PermissionResponse`], and an absent key fails the field table
    /// with the same message.
    response: Option<Value>,
}

impl Params for Respond {
    const METHOD: &'static str = "permission.respond";
    const FIELDS: &'static [Field] = &[
        Field::str("sessionId"),
        Field::str("requestId"),
        Field::json("response"),
    ];
}

pub(super) async fn respond(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    // Wire order preserved: the live session is resolved before the
    // request's other fields are checked.
    let id = Respond::peek_str(params, "sessionId")?;
    let ms = state
        .sessions
        .get(id)
        .await
        .ok_or_else(|| RpcError::error(error_code::SESSION_NOT_LIVE, "session not live"))?;
    let req = Respond::parse(params)?;
    let request_id = req.request_id.as_str();
    let response: PermissionResponse =
        serde_json::from_value(req.response.clone().unwrap_or(Value::Null))
            .map_err(|_| RpcError::error(error_code::INVALID_PARAMS, "response required"))?;
    // Plan asks land their decision in the durable review row too —
    // intent BEFORE the native write (the "lost pipe" record), settled
    // on ack, reverted on failure. The gate's winner owns the record:
    // if plan.respond claimed it first, this legacy call proceeds
    // natively without touching the row (the backend's one-shot ask
    // dedups the double delivery anyway). Legacy semantics are untouched
    // for non-plan asks and for plans with no open row.
    let mut plan_claim: Option<(String, i64, String, String)> = None;
    if let Ok(Some(row)) = state.store.open_plan_for_request(request_id).await {
        let (decision, feedback) = match &response {
            PermissionResponse::Allow { .. } => ("approve", None),
            PermissionResponse::Deny { message, .. } => ("request_changes", message.clone()),
        };
        if let Ok(crate::store::PlanSubmit::Claimed { previous, .. }) = state
            .store
            .submit_plan_decision(&row.plan_id, row.revision, decision, feedback.as_deref())
            .await
        {
            plan_claim = Some((
                row.plan_id.clone(),
                row.revision,
                decision.to_string(),
                previous,
            ));
        }
    }
    let delivered = ms
        .session
        .respond_to_permission(request_id, response.clone())
        .await;
    if let Some((pid, rev, decision, previous)) = plan_claim {
        match &delivered {
            Ok(()) => {
                let _ = state.store.settle_plan(&pid, rev, &decision).await;
            }
            Err(_) => {
                let _ = state.store.revert_plan(&pid, rev, &previous).await;
            }
        }
    }
    delivered?;
    // A deny with `interrupt` kills the whole turn, not just this call —
    // backends only apply the flag to the pending ask, so the daemon
    // drives the follow-up interrupt here. Best-effort: the deny itself
    // already took effect.
    if matches!(
        response,
        PermissionResponse::Deny {
            interrupt: true,
            ..
        }
    ) && let Err(e) = ms.session.interrupt().await
    {
        tracing::warn!(error = %e, session = %id, "post-deny interrupt failed");
    }
    Ok(json!({}))
}

// ─────────────────────────────────────────────────────────────────────
// The family contract, for the drift tests
// ─────────────────────────────────────────────────────────────────────

/// Every method in this module with the fields its struct reads.
#[cfg(test)]
pub(super) const FAMILY: &[(&str, &[Field])] = &[(Respond::METHOD, Respond::FIELDS)];

/// Parse the family's method through its typed struct — the seam the
/// drift tests drive.
#[cfg(test)]
pub(super) fn parse_family(method: &str, params: &Value) -> Result<()> {
    match method {
        Respond::METHOD => Respond::parse(params).map(drop),
        other => panic!("{other} is in FAMILY but has no typed parse"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::params::drift;
    use super::{FAMILY, parse_family};

    /// The catalog's `permission.*` entries and the struct in this module
    /// are one contract, checked both ways (see `params::drift`).
    #[test]
    fn catalog_declares_exactly_the_fields_the_structs_read() {
        drift::catalog_matches(FAMILY, &["permission."]);
    }

    /// The table is what parse enforces.
    #[test]
    fn the_declared_fields_are_the_fields_that_are_checked() {
        drift::fields_are_checked(FAMILY, parse_family);
    }

    /// The documented shapes parse, including the question-card answers
    /// map `docs/protocol-v2.md` publishes.
    #[test]
    fn documented_requests_parse() {
        for params in [
            json!({"sessionId": "s", "requestId": "r",
                   "response": {"behavior": "allow"}}),
            json!({"sessionId": "s", "requestId": "r",
                   "response": {"behavior": "deny", "message": "no",
                                "interrupt": true}}),
            json!({"sessionId": "s", "requestId": "r",
                   "response": {"behavior": "allow",
                                "answers": {"Which DB?": "SQLite"}}}),
        ] {
            parse_family("permission.respond", &params).unwrap_or_else(|e| panic!("{params}: {e}"));
        }
    }
}
