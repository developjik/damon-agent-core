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
//! A method family moves to one struct per method instead (`rpc::session`,
//! `rpc::turn`, `rpc::permission`, `rpc::plan` so far). Each struct derives
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
//!   `--print-rpc-schema` catalog), in both directions, by each family's
//!   `catalog_declares_exactly_the_fields_the_structs_read` test — one
//!   call into `drift::catalog_matches`, which the families share. A
//!   field a handler reads but the schema does not name — the drift this
//!   refactor exists to stop — fails the build.

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
    /// A required field the handler deserializes into its own type
    /// (`turn.start`'s `prompt`, `permission.respond`'s `response`): the
    /// table checks that the key is there, the handler's deserializer
    /// decides whether the value fits, and both answer the one legacy
    /// message (`"prompt required"`).
    pub(super) const fn json(name: &'static str) -> Self {
        Self::new(name, Kind::Json, true)
    }
    /// A required integer (`as_i64`), for the `-32602` a numeric param
    /// owes when it is missing or not a number.
    pub(super) const fn int(name: &'static str) -> Self {
        Self::new(name, Kind::Int, true)
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

    /// This method's table entry for one declared field. Panics on a name
    /// the table does not carry — a programming error, which the drift
    /// tests that walk `FAMILY` make a build failure.
    fn field(name: &'static str) -> &'static Field {
        Self::FIELDS
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("{} has no field `{name}`", Self::METHOD))
    }

    /// Read one declared string field *ahead of* [`parse`](Self::parse),
    /// with the same table entry and the same message.
    ///
    /// `turn.start`, `turn.steer` and `permission.respond` resolve the
    /// live session before they validate the rest of the request — the
    /// pre-refactor wire order, where a dead session answers
    /// `SESSION_NOT_LIVE` and a Keyed retry replays its first answer
    /// without the prompt ever being looked at. Those reads come through
    /// this seam so the field table stays the one description of the
    /// params; `parse` still runs on the whole table afterwards.
    fn peek_str<'a>(params: &'a Value, name: &'static str) -> anyhow::Result<&'a str> {
        let field = Self::field(name);
        match params.get(name) {
            Some(v) if field.accepts(v) => match v.as_str() {
                Some(s) => Ok(s),
                // `accepts` already said this is a string.
                None => Err(RpcError::error(error_code::INVALID_PARAMS, field.message())),
            },
            _ => Err(RpcError::error(error_code::INVALID_PARAMS, field.message())),
        }
    }

    /// The optional twin of [`peek_str`](Self::peek_str): `None` for an
    /// absent or ill-typed field, exactly what the untyped `as_str()`
    /// read answered.
    fn peek_str_opt<'a>(params: &'a Value, name: &'static str) -> Option<&'a str> {
        let field = Self::field(name);
        params.get(name).filter(|v| field.accepts(v))?.as_str()
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

    /// `peek_str`/`peek_str_opt` answer off the same table entry as
    /// `parse`: the required one with the field's legacy message, the
    /// optional one silently `None`.
    #[test]
    fn peek_reads_one_field_with_the_table_entry() {
        let params = json!({"sessionId": "s"});
        assert_eq!("s", Sample::peek_str(&params, "sessionId").unwrap());
        for params in [
            json!({}),
            json!({"sessionId": 7}),
            json!({"sessionId": null}),
        ] {
            let err = Sample::peek_str(&params, "sessionId").unwrap_err();
            assert_eq!("sessionId required", format!("{err}"), "for {params}");
        }
        assert_eq!(
            None,
            Sample::peek_str_opt(&json!({}), "sessionId"),
            "absent reads as None"
        );
        assert_eq!(
            None,
            Sample::peek_str_opt(&json!({"sessionId": 7}), "sessionId"),
            "ill-typed reads as None, like as_str()"
        );
        assert_eq!(
            Some("s"),
            Sample::peek_str_opt(&json!({"sessionId": "s"}), "sessionId")
        );
    }
}

/// The family-vs-catalog contract, shared by the tests of every family
/// module (`rpc::session`, `rpc::turn`, `rpc::permission`, `rpc::plan`, …).
///
/// A family hands in its `FAMILY` table, the method-name prefixes it owns
/// and its `parse_family` seam; the checks are the two directions
/// `session::tests` spelled out for the first slice: the catalog must
/// name exactly what the structs read (same name, optionality and JSON
/// type), and the table must be what parse enforces.
#[cfg(test)]
pub(super) mod drift {
    use serde_json::{Map, Value, json};

    use super::{Field, Kind};
    use crate::rpc::rpc_methods;

    /// The catalog's type token → the kind the struct reads it as.
    /// `None` means "not a type this module can compare" (a prose entry).
    pub(crate) fn kind_of(token: &str) -> Option<Kind> {
        match token {
            "string" => Some(Kind::Str),
            "number" => Some(Kind::UInt),
            "boolean" => Some(Kind::Bool),
            "string[]" => Some(Kind::StrArray),
            "object" => Some(Kind::Object),
            // `turn.start`/`turn.steer` take text *or* a block array; the
            // struct keeps the raw JSON and the handler deserializes it,
            // so the catalog token is compared as the free-form kind.
            "string|array" => Some(Kind::Json),
            t if t.starts_with('{') => Some(Kind::Object),
            _ => None,
        }
    }

    /// A value of the declared kind — the params a client would send.
    pub(crate) fn sample(kind: Kind) -> Value {
        match kind {
            Kind::Str => json!("sample"),
            Kind::UInt | Kind::Int => json!(1),
            Kind::Bool => json!(true),
            Kind::StrArray | Kind::Array => json!(["a"]),
            Kind::Object => json!({}),
            Kind::Json => json!({}),
        }
    }

    /// The catalog entry for one method, params object only.
    pub(crate) fn catalog_params(method: &str) -> Map<String, Value> {
        let entry = rpc_methods()
            .into_iter()
            .find(|m| m["name"] == json!(method))
            .unwrap_or_else(|| panic!("{method} missing from rpc_methods()"));
        entry["params"]
            .as_object()
            .cloned()
            .unwrap_or_else(|| panic!("{method} advertises no params object"))
    }

    /// The schema catalog and the typed params are one contract, checked
    /// both ways: every param `hello` advertises is a field the struct
    /// reads (same name, same optionality, same JSON type), and every
    /// field a struct reads is advertised. A handler reading a param the
    /// catalog never named — the drift this refactor exists to stop —
    /// fails here, as does a catalog entry left behind by a removed
    /// field, and a method one side knows and the other never heard of.
    pub(crate) fn catalog_matches(family: &[(&'static str, &'static [Field])], prefixes: &[&str]) {
        // Every catalog entry under this family's prefixes belongs to it.
        let mut advertised: Vec<String> = rpc_methods()
            .into_iter()
            .filter_map(|m| m["name"].as_str().map(str::to_string))
            .filter(|n| prefixes.iter().any(|p| n.starts_with(p)))
            .collect();
        let mut listed: Vec<String> = family.iter().map(|(m, _)| m.to_string()).collect();
        advertised.sort();
        listed.sort();
        assert_eq!(
            advertised, listed,
            "catalog and FAMILY disagree on the set of methods"
        );

        for (method, fields) in family {
            let params = catalog_params(method);
            // Catalog → struct.
            for (name, desc) in &params {
                let desc = desc.as_str().unwrap_or_default();
                // The type token is the head of the description, before
                // the em-dash note ("string? — one backend, else all").
                let head = desc.split(" — ").next().unwrap_or_default().trim();
                let (token, optional) = match head.strip_suffix('?') {
                    Some(t) => (t.trim(), true),
                    None => (head, false),
                };
                let kind = kind_of(token)
                    .unwrap_or_else(|| panic!("{method}.{name}: unreadable type {desc:?}"));
                let field = fields.iter().find(|f| f.name == name).unwrap_or_else(|| {
                    panic!("{method} advertises `{name}` but reads no such field")
                });
                assert_eq!(
                    field.kind.schema_type(),
                    kind.schema_type(),
                    "{method}.{name}: catalog type and struct type disagree"
                );
                if kind == Kind::StrArray || kind == Kind::Array {
                    assert!(
                        matches!(field.kind, Kind::StrArray | Kind::Array),
                        "{method}.{name}: array kind mismatch"
                    );
                }
                assert_eq!(
                    !field.required, optional,
                    "{method}.{name}: catalog optionality and struct requiredness disagree"
                );
            }
            // Struct → catalog.
            for field in fields.iter() {
                assert!(
                    params.contains_key(field.name),
                    "{method} reads `{}` but the catalog never names it",
                    field.name
                );
            }
        }
    }

    /// The field table is not decoration: dropping a required field fails
    /// the call with its legacy message, an optional field is optional.
    /// Driven generically off the family table, so it covers every method
    /// the family grows.
    pub(crate) fn fields_are_checked(
        family: &[(&'static str, &'static [Field])],
        parse_family: fn(&str, &Value) -> anyhow::Result<()>,
    ) {
        for (method, fields) in family {
            // All declared fields present, correctly typed: the call gets
            // through parameter validation. (One method may want one of
            // two shapes; both present is still a valid shape.)
            let mut all = Map::new();
            for f in fields.iter() {
                all.insert(f.name.to_string(), sample(f.kind));
            }
            let all = Value::Object(all);
            if let Err(e) = parse_family(method, &all) {
                let msg = format!("{e}");
                // A required field the table declares must be accepted —
                // any other rejection is a handler-level rule (unknown
                // backend, unparseable bound), not a params check.
                assert!(
                    !msg.ends_with("required") && !msg.contains("must be"),
                    "{method}: declared-everything params were refused: {msg}"
                );
            }

            for f in fields.iter().filter(|f| f.required) {
                let mut missing = all.as_object().cloned().unwrap_or_default();
                missing.remove(f.name);
                let err = parse_family(method, &Value::Object(missing))
                    .expect_err(&format!("{method} accepted params without `{}`", f.name));
                assert_eq!(
                    f.message(),
                    format!("{err}"),
                    "{method}: wrong message without `{}`",
                    f.name
                );
                // Ill-typed reads exactly like absent for a required
                // field — except for the free-form kinds, where nothing
                // is ill-typed (the handler's deserializer judges the
                // value and answers the same message there).
                if f.kind == Kind::Json {
                    continue;
                }
                let mut wrong = all.as_object().cloned().unwrap_or_default();
                wrong.insert(
                    f.name.to_string(),
                    match f.kind {
                        Kind::Str => json!(7),
                        Kind::Bool => json!("yes"),
                        Kind::StrArray | Kind::Array => json!("nope"),
                        _ => json!(null),
                    },
                );
                let err = parse_family(method, &Value::Object(wrong))
                    .expect_err(&format!("{method} accepted a mistyped `{}`", f.name));
                assert_eq!(f.message(), format!("{err}"), "{method} `{}`", f.name);
            }

            for f in fields.iter().filter(|f| !f.required) {
                // An ill-typed optional param is dropped, not refused.
                let mut wrong = all.as_object().cloned().unwrap_or_default();
                wrong.insert(f.name.to_string(), json!({"not": "the type"}));
                if f.kind == Kind::Object || f.kind == Kind::Json {
                    // An object is a valid Json/Object value: swap in a
                    // scalar instead.
                    wrong.insert(f.name.to_string(), json!(7));
                }
                if let Err(e) = parse_family(method, &Value::Object(wrong)) {
                    let msg = format!("{e}");
                    assert!(
                        msg.ends_with("required") || msg.contains("must be"),
                        "{method}: optional `{}` was refused: {msg}",
                        f.name
                    );
                }
            }
        }
    }
}
