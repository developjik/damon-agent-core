//! Typed request parameters — the shared half of the `dispatch` refactor.
//!
//! `dispatch` reads its request out of an untyped `serde_json::Value`
//! (`params["sessionId"].as_str()`, `req_str(...)`). That is where the
//! 100-plus unchecked reads in `rpc.rs` came from: a key name typo is a
//! silent `None` (a `sessionId`-less call quietly falls through to the
//! wrong branch), a field the handler reads but the schema in
//! `rpc_methods()` never advertises is invisible, and nothing pins the
//! two together.
//!
//! A method family moves to one struct per method instead (see
//! `rpc::session` for the first family). Each struct derives
//! [`serde::Deserialize`] — the shape lives in the type — and declares a
//! [`Field`] table describing what it reads, which [`Params::parse`]
//! enforces. Two properties are deliberate:
//!
//! * **Wire compatibility.** A missing or ill-typed *required* field
//!   answers `-32602` with the exact message the pre-refactor handler
//!   sent (`"sessionId required"`, `"pinned must be a boolean"`, …) —
//!   `docs/protocol-v2.md` publishes that text as the error example and
//!   `tests/rpc.rs` asserts on it. An ill-typed *optional* field is
//!   dropped rather than refused, because `params["limit"].as_u64()` was
//!   silently `None` too; `format!("{value}")` on a wrong type is not.
//! * **Schema drift is testable.** The field table is compared against
//!   the method's hand-written entry in `rpc_methods()` (the `hello` /
//!   `--print-rpc-schema` catalog), in both directions, by
//!   `session::tests::catalog_declares_exactly_the_fields_the_structs_read`.
//!   A field a handler reads but the schema does not name — the drift
//!   this refactor exists to stop — fails the build.

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use super::{RpcError, error_code};

/// The JSON type a wire field must carry. `UInt`/`Int` are the two
/// numeric reads the daemon performs (`as_u64`, `as_i64`); both report
/// `number` in the schema catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// A JSON string (`as_str`).
    Str,
    /// A non-negative integer (`as_u64`).
    UInt,
    /// A possibly negative integer (`as_i64`).
    Int,
    /// A JSON boolean (`as_bool`).
    Bool,
    /// An array of strings (`serde_json::from_value::<Vec<String>>`).
    StrArray,
    /// A JSON object (`as_object`).
    Object,
    /// Any array (`as_array`); element types are the handler's business.
    Array,
    /// Anything the key holds, verbatim — including `null` (see
    /// [`Present`]).
    Json,
}

impl Kind {
    /// How this kind is spelled in the `rpc_methods()` catalog: the
    /// type token at the head of a field's description. Test-only — the
    /// catalog is compared against it, the wire never renders it.
    #[cfg(test)]
    pub(super) const fn schema_type(self) -> &'static str {
        match self {
            Kind::Str => "string",
            Kind::UInt | Kind::Int => "number",
            Kind::Bool => "boolean",
            Kind::StrArray | Kind::Array => "string[]",
            Kind::Object | Kind::Json => "object",
        }
    }
}

/// One field of a method's request object: the wire name, the JSON type
/// it is read as, and whether the call needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Field {
    pub(super) name: &'static str,
    pub(super) kind: Kind,
    pub(super) required: bool,
}

impl Field {
    pub(super) const fn str(name: &'static str) -> Self {
        Self::new(name, Kind::Str, true)
    }
    pub(super) const fn str_opt(name: &'static str) -> Self {
        Self::new(name, Kind::Str, false)
    }
    pub(super) const fn uint_opt(name: &'static str) -> Self {
        Self::new(name, Kind::UInt, false)
    }
    pub(super) const fn int_opt(name: &'static str) -> Self {
        Self::new(name, Kind::Int, false)
    }
    pub(super) const fn bool(name: &'static str) -> Self {
        Self::new(name, Kind::Bool, true)
    }
    pub(super) const fn bool_opt(name: &'static str) -> Self {
        Self::new(name, Kind::Bool, false)
    }
    pub(super) const fn str_array(name: &'static str) -> Self {
        Self::new(name, Kind::StrArray, true)
    }
    pub(super) const fn array_opt(name: &'static str) -> Self {
        Self::new(name, Kind::Array, false)
    }
    pub(super) const fn object_opt(name: &'static str) -> Self {
        Self::new(name, Kind::Object, false)
    }
    pub(super) const fn json_opt(name: &'static str) -> Self {
        Self::new(name, Kind::Json, false)
    }

    const fn new(name: &'static str, kind: Kind, required: bool) -> Self {
        Self {
            name,
            kind,
            required,
        }
    }

    /// The message the pre-refactor handler sent when this field was
    /// missing or carried the wrong JSON type. Byte-identical on
    /// purpose — `req_str` renders `"{key} required"` and the boolean /
    /// tag reads render their own text; clients and the protocol docs
    /// match on it.
    pub(super) fn message(&self) -> String {
        match self.kind {
            Kind::Str | Kind::Object | Kind::Json => format!("{} required", self.name),
            Kind::Bool => format!("{} must be a boolean", self.name),
            Kind::StrArray => format!("{} must be an array of strings", self.name),
            // The untyped reads defaulted numeric params instead of
            // refusing them, so no method requires one yet; the text
            // keeps a future required number honest.
            Kind::UInt | Kind::Int => format!("{} must be a number", self.name),
            Kind::Array => format!("{} must be an array", self.name),
        }
    }

    /// Whether `v` is what [`Params::parse`] may hand to the struct.
    fn accepts(&self, v: &Value) -> bool {
        match self.kind {
            Kind::Str => v.is_string(),
            Kind::UInt => v.as_u64().is_some(),
            Kind::Int => v.as_i64().is_some(),
            Kind::Bool => v.is_boolean(),
            Kind::StrArray => v.as_array().is_some_and(|a| a.iter().all(Value::is_string)),
            Kind::Object => v.is_object(),
            Kind::Array => v.is_array(),
            Kind::Json => true,
        }
    }
}

/// A request field whose *presence* is the signal, whatever it holds.
/// `session.resume` needs it: a `handle` key (even `null`) selects the
/// import path and is validated there, so `Option<Value>` — which reads
/// `null` as absent — would answer the wrong error for `{"handle":
/// null}`.
#[derive(Debug, Default)]
pub(super) struct Present<T>(pub(super) Option<T>);

impl<'de> Deserialize<'de> for Present<Value> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self(Some(Value::deserialize(d)?)))
    }
}

/// A typed request: the struct a method's params deserialize into, plus
/// the field table that is both its validation rule and its schema
/// catalog entry.
pub(super) trait Params: DeserializeOwned {
    /// The wire method name this request belongs to (error context).
    const METHOD: &'static str;
    /// Every field the struct reads, in wire order. Must describe the
    /// struct exactly — the family's drift tests hold both sides to it.
    const FIELDS: &'static [Field];

    /// Deserialize `params` into `Self`.
    ///
    /// Declared fields are copied into a clean object after the field
    /// table has checked them; everything else — undeclared keys, and
    /// ill-typed optional fields — is dropped, which is what the
    /// untyped `params["k"].as_*()` reads did. The struct therefore
    /// needs `#[serde(default)]` on every optional field.
    fn parse(params: &Value) -> anyhow::Result<Self> {
        let given = params.as_object().cloned().unwrap_or_default();
        let mut clean = Map::new();
        for field in Self::FIELDS {
            match given.get(field.name) {
                Some(v) if field.accepts(v) => {
                    clean.insert(field.name.to_string(), v.clone());
                }
                // Absent or ill-typed: the untyped read failed the same
                // way for both, so both get the one message — and only
                // where the call cannot go on without the field.
                _ if field.required => {
                    return Err(RpcError::error(error_code::INVALID_PARAMS, field.message()));
                }
                _ => {}
            }
        }
        serde_json::from_value(Value::Object(clean)).map_err(|e| {
            // Unreachable while `FIELDS` describes the struct; kept a
            // typed error rather than a panic for the day it does not.
            RpcError::error(
                error_code::INVALID_PARAMS,
                format!("invalid {} params: {e}", Self::METHOD),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::{Field, Kind, Params};
    use crate::rpc::{RpcError, error_code, req_str};

    #[derive(Debug, Default, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    struct Sample {
        session_id: String,
        pinned: bool,
        tags: Vec<String>,
        limit: Option<u64>,
    }

    impl Params for Sample {
        const METHOD: &'static str = "sample.thing";
        const FIELDS: &'static [Field] = &[
            Field::str("sessionId"),
            Field::bool("pinned"),
            Field::str_array("tags"),
            Field::uint_opt("limit"),
        ];
    }

    /// The typed layer's required-field messages are the untyped
    /// `req_str` text, character for character — that is the contract
    /// `docs/protocol-v2.md` publishes and clients match on.
    #[test]
    fn required_messages_match_the_untyped_reads() {
        let legacy = req_str(&json!({}), "sessionId").unwrap_err();
        assert_eq!(format!("{legacy}"), Field::str("sessionId").message());
        assert_eq!("pinned must be a boolean", Field::bool("pinned").message());
        assert_eq!(
            "tags must be an array of strings",
            Field::str_array("tags").message()
        );
    }

    #[test]
    fn a_missing_required_field_is_refused_with_its_legacy_message() {
        for (params, message) in [
            (json!({}), "sessionId required"),
            (json!({"sessionId": 7}), "sessionId required"),
            (json!({"sessionId": "s"}), "pinned must be a boolean"),
            (
                json!({"sessionId": "s", "pinned": true}),
                "tags must be an array of strings",
            ),
            (
                json!({"sessionId": "s", "pinned": true, "tags": ["a", 2]}),
                "tags must be an array of strings",
            ),
        ] {
            let err = Sample::parse(&params).unwrap_err();
            let typed = err
                .downcast_ref::<RpcError>()
                .expect("typed error on the wire");
            assert_eq!(error_code::INVALID_PARAMS, typed.code);
            assert_eq!(message, format!("{err}"), "for {params}");
        }
    }

    /// An ill-typed *optional* param was silently `None` before and is
    /// still: refusing it would turn a working call into an error.
    #[test]
    fn ill_typed_optional_fields_are_ignored_not_refused() {
        let s = Sample::parse(&json!({
            "sessionId": "s", "pinned": true, "tags": [],
            "limit": "ten",
        }))
        .expect("optional junk is dropped");
        assert_eq!(None, s.limit);
        // Right type, still there.
        let s = Sample::parse(&json!({
            "sessionId": "s", "pinned": true, "tags": [], "limit": 5,
        }))
        .unwrap();
        assert_eq!(Some(5), s.limit);
    }

    /// Undeclared keys (a client sending a field this daemon does not
    /// know) are ignored, not refused — additive params stay additive.
    #[test]
    fn undeclared_keys_are_dropped() {
        let s = Sample::parse(&json!({
            "sessionId": "s", "pinned": false, "tags": ["x"], "future": {"a": 1},
        }))
        .unwrap();
        assert_eq!("s", s.session_id);
        assert_eq!(vec!["x".to_string()], s.tags);
    }

    /// A non-object `params` (a malformed frame) reads as an empty
    /// object, matching `params["k"]` on a non-object value.
    #[test]
    fn non_object_params_read_as_empty() {
        let err = Sample::parse(&json!([1, 2])).unwrap_err();
        assert_eq!("sessionId required", format!("{err}"));
    }

    #[test]
    fn kinds_report_the_catalog_type_tokens() {
        assert_eq!("string", Kind::Str.schema_type());
        assert_eq!("number", Kind::UInt.schema_type());
        assert_eq!("number", Kind::Int.schema_type());
        assert_eq!("boolean", Kind::Bool.schema_type());
        assert_eq!("string[]", Kind::StrArray.schema_type());
        assert_eq!("object", Kind::Object.schema_type());
    }
}
