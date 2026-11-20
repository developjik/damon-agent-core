//! `plan.*` — the plan-review family: what the review surface lists, and
//! the one-winner decision (see [`super::params`] for the typed-params
//! machinery this family follows).
//!
//! `plan.respond` is the CAS gate the protocol doc describes: the call
//! claims `expectedRevision` before anything reaches the agent, and the
//! store — not this module — decides who wins. `permission.respond` can
//! record the same decision on a `kind: plan` ask; whichever reaches the
//! row first owns it.
//!
//! Wire behavior is unchanged on purpose — codes, messages and the
//! defaults included.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::AppState;
use crate::backend::types::PermissionResponse;

use super::params::{Field, Params};
use super::{RpcError, error_code};

// ─────────────────────────────────────────────────────────────────────
// Requests
// ─────────────────────────────────────────────────────────────────────

/// `plan.list` — every revision of every plan in one session.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct List {
    session_id: String,
}

impl Params for List {
    const METHOD: &'static str = "plan.list";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `plan.respond` — claim and record one review decision.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Respond {
    plan_id: String,
    /// The revision the caller believes it is deciding — the CAS token.
    expected_revision: i64,
    decision: String,
    /// Required non-empty for `request_changes`; rides the deny as the
    /// agent's revision guidance.
    feedback: Option<String>,
}

impl Params for Respond {
    const METHOD: &'static str = "plan.respond";
    const FIELDS: &'static [Field] = &[
        Field::str("planId"),
        Field::int("expectedRevision"),
        Field::str("decision"),
        Field::str_opt("feedback"),
    ];
}

// ─────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────

pub(super) async fn list(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = List::parse(params)?;
    let rows = state.store.plan_reviews(&req.session_id).await?;
    let plans: Vec<Value> = rows.into_iter().map(plan_row_json).collect();
    Ok(json!({"plans": plans}))
}

pub(super) async fn respond(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Respond::parse(params)?;
    let plan_id = req.plan_id.as_str();
    let expected_revision = req.expected_revision;
    let decision = req.decision.as_str();
    if !matches!(decision, "approve" | "request_changes" | "defer") {
        return Err(RpcError::error(
            error_code::INVALID_PARAMS,
            "decision must be approve | request_changes | defer",
        ));
    }
    let feedback = req.feedback.as_deref();
    match state
        .store
        .submit_plan_decision(plan_id, expected_revision, decision, feedback)
        .await?
    {
        crate::store::PlanSubmit::FeedbackRequired => Err(RpcError::error(
            error_code::INVALID_PARAMS,
            "request_changes requires non-empty feedback",
        )),
        crate::store::PlanSubmit::Incomplete => Err(RpcError::error(
            error_code::INVALID_PARAMS,
            "plan content is not reviewable — approval refused",
        )),
        crate::store::PlanSubmit::NotFound => Err(RpcError::error(
            error_code::INVALID_PARAMS,
            format!("no plan review {plan_id} revision {expected_revision}"),
        )),
        crate::store::PlanSubmit::Conflict(row) => Err(RpcError::error(
            error_code::PLAN_CONFLICT,
            format!(
                "plan decision conflict — revision {} is {} (expected revision {}); refetch via plan.list",
                row.revision, row.status, expected_revision
            ),
        )),
        crate::store::PlanSubmit::Claimed {
            session_id,
            request_id,
            previous,
        } => {
            // Deferred is non-terminal and never touches the native ask:
            // the agent's wait stays parked and a later plan.respond can
            // still decide it.
            if decision == "defer" {
                state
                    .store
                    .settle_plan(plan_id, expected_revision, decision)
                    .await?;
                return Ok(json!({
                    "recorded": true, "status": "deferred", "applied": false,
                    "planId": plan_id, "revision": expected_revision,
                }));
            }
            let ms = state.sessions.get(&session_id).await;
            let Some(ms) = ms else {
                // The ask died with the session — the intent stays
                // recorded, the row returns to its pre-claim state.
                let _ = state
                    .store
                    .revert_plan(plan_id, expected_revision, &previous)
                    .await;
                return Err(RpcError::error(
                    error_code::SESSION_NOT_LIVE,
                    "session not live — the plan ask died with it",
                ));
            };
            let response = if decision == "approve" {
                PermissionResponse::Allow {
                    action_id: None,
                    updated_input: None,
                    answer: None,
                    answers: None,
                }
            } else {
                PermissionResponse::Deny {
                    action_id: None,
                    // The feedback IS the message the agent sees for its
                    // next revision.
                    message: Some(feedback.unwrap_or_default().to_string()),
                    interrupt: false,
                }
            };
            match ms
                .session
                .respond_to_permission(&request_id, response)
                .await
            {
                Ok(()) => {
                    state
                        .store
                        .settle_plan(plan_id, expected_revision, decision)
                        .await?;
                    Ok(json!({
                        "recorded": true,
                        "status": if decision == "approve" { "approved" } else { "changes_requested" },
                        "applied": true,
                        "planId": plan_id, "revision": expected_revision,
                    }))
                }
                Err(e) => {
                    // Relay lost: the intent stays on file, the row
                    // reopens, and the caller sees the failure instead of
                    // a fake success.
                    let _ = state
                        .store
                        .revert_plan(plan_id, expected_revision, &previous)
                        .await;
                    Err(RpcError::error(
                        error_code::INTERNAL_ERROR,
                        format!("plan decision recorded but native relay failed: {e:#}"),
                    ))
                }
            }
        }
    }
}

/// One plan-review row on the wire: every revision, chain-ordered, with
/// the decision fields present only once they exist.
fn plan_row_json(r: crate::store::PlanRow) -> Value {
    let mut v = json!({
        "planId": r.plan_id,
        "sessionId": r.session_id,
        "requestId": r.request_id,
        "revision": r.revision,
        "title": r.title,
        "content": r.content,
        "contentHash": r.content_hash,
        "complete": r.complete,
        "status": r.status,
        "createdAt": r.created_at,
        "updatedAt": r.updated_at,
    });
    if let Some(d) = r.decision {
        v["decision"] = d;
    }
    if let Some(t) = r.decision_intent_at {
        v["decisionIntentAt"] = json!(t);
    }
    if let Some(t) = r.applied_at {
        v["appliedAt"] = json!(t);
    }
    if let Some(s) = r.superseded_by {
        v["supersededBy"] = json!(s);
    }
    v
}

// ─────────────────────────────────────────────────────────────────────
// The family contract, for the drift tests
// ─────────────────────────────────────────────────────────────────────

/// Every method in this module with the fields its struct reads.
#[cfg(test)]
pub(super) const FAMILY: &[(&str, &[Field])] = &[
    (List::METHOD, List::FIELDS),
    (Respond::METHOD, Respond::FIELDS),
];

/// Parse one family method's params through its typed struct — the seam
/// the drift tests drive.
#[cfg(test)]
pub(super) fn parse_family(method: &str, params: &Value) -> Result<()> {
    match method {
        List::METHOD => List::parse(params).map(drop),
        Respond::METHOD => Respond::parse(params).map(drop),
        other => panic!("{other} is in FAMILY but has no typed parse"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::params::drift;
    use super::{FAMILY, Params, Respond, parse_family};

    /// The catalog's `plan.*` entries and the structs in this module are
    /// one contract, checked both ways (see `params::drift`).
    #[test]
    fn catalog_declares_exactly_the_fields_the_structs_read() {
        drift::catalog_matches(FAMILY, &["plan."]);
    }

    /// The table is what parse enforces.
    #[test]
    fn the_declared_fields_are_the_fields_that_are_checked() {
        drift::fields_are_checked(FAMILY, parse_family);
    }

    /// The documented shapes parse — all three decisions, with and
    /// without feedback.
    #[test]
    fn documented_requests_parse() {
        for (method, params) in [
            ("plan.list", json!({"sessionId": "s"})),
            (
                "plan.respond",
                json!({"planId": "p", "expectedRevision": 2, "decision": "approve"}),
            ),
            (
                "plan.respond",
                json!({"planId": "p", "expectedRevision": 2,
                       "decision": "request_changes", "feedback": "split it"}),
            ),
            (
                "plan.respond",
                json!({"planId": "p", "expectedRevision": 2, "decision": "defer"}),
            ),
        ] {
            parse_family(method, &params).unwrap_or_else(|e| panic!("{method} {params}: {e}"));
        }
    }

    /// `expectedRevision` is a number and nothing numeric-looking: a
    /// string, a fraction or a float answers the historical message
    /// (the untyped `as_i64()` refused all three).
    #[test]
    fn expected_revision_is_an_integer() {
        for bad in [
            json!({"planId": "p", "decision": "approve"}),
            json!({"planId": "p", "expectedRevision": "2", "decision": "approve"}),
            json!({"planId": "p", "expectedRevision": 2.5, "decision": "approve"}),
        ] {
            let err = Respond::parse(&bad).unwrap_err();
            assert_eq!(
                "expectedRevision must be a number",
                format!("{err}"),
                "for {bad}"
            );
        }
        assert_eq!(
            2,
            Respond::parse(&json!({"planId": "p", "expectedRevision": 2, "decision": "defer"}))
                .unwrap()
                .expected_revision
        );
    }
}
