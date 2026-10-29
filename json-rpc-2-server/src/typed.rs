//! Typed method parameters: deserialization, validation and JSON Schema documentation.
//!
//! Типізовані параметри методів: десеріалізація, перевірка й документація JSON Schema.
//!
//! ```
//! use json_rpc_2_server::{NonBlank, RpcServer};
//! use schemars::JsonSchema;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize, JsonSchema)]
//! #[schemars(example = serde_json::json!({"name": "Ada"}))]
//! struct Hello {
//!     name: NonBlank,
//! }
//!
//! let server = RpcServer::new();
//! server.register_typed("hello", (), |(), params: Hello| async move {
//!     Ok(serde_json::json!(format!("Hello, {}", params.name.as_str())))
//! })?;
//! # Ok::<(), json_rpc_2_server::RegistrationError>(())
//! ```

use std::{borrow::Cow, ops::Deref};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema, schema_for};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned, de::Error as _};
use serde_json::Value;

use crate::{RegistrationError, RpcError, RpcResult, RpcServer};

/// Deserializes params; any mismatch is the standard `-32602 Invalid params` error.
/// Десеріалізує параметри; будь-яка невідповідність дає стандартну помилку `-32602`.
pub fn parse_params<P: DeserializeOwned>(params: Value) -> RpcResult<P> {
    serde_json::from_value(params).map_err(|_| RpcError::invalid_params())
}

/// JSON Schema of `P` and its first `#[schemars(example = ...)]` value (`null` if absent).
/// JSON Schema типу `P` і перший приклад з `#[schemars(example = ...)]` (`null`, якщо немає).
pub fn params_documentation<P: JsonSchema>() -> (Value, Value) {
    let schema = schema_for!(P).to_value();
    let example = schema
        .get("examples")
        .and_then(|examples| examples.get(0))
        .cloned()
        .unwrap_or(Value::Null);
    (schema, example)
}

impl RpcServer {
    /// Registers a method whose params are deserialized into `P` and documents it with
    /// `P`'s JSON Schema and example. Like [`register_with`](Self::register_with), the
    /// handler receives a clone of `state`. Register aliases afterwards.
    pub fn register_typed<S, P, R, F, Fut>(
        &self,
        name: &str,
        state: S,
        handler: F,
    ) -> Result<(), RegistrationError>
    where
        S: Clone + Send + Sync + 'static,
        P: DeserializeOwned + JsonSchema + Send + 'static,
        R: Serialize,
        F: Fn(S, P) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = RpcResult<R>> + Send + 'static,
    {
        self.register_with(name, state, move |state, params| {
            let handler = handler.clone();
            async move {
                let result = handler(state, parse_params(params)?).await?;
                serde_json::to_value(result).map_err(|_| RpcError::internal_error())
            }
        })?;
        self.document_params::<P>(name)
    }

    /// Documents an already registered method with `P`'s JSON Schema and example,
    /// e.g. when the handler must inspect raw params before [`parse_params`].
    pub fn document_params<P: JsonSchema>(&self, name: &str) -> Result<(), RegistrationError> {
        let (schema, example) = params_documentation::<P>();
        self.document_method(name, schema, example)
    }
}

/// A string with at least one non-whitespace character, kept exactly as sent.
/// Рядок принаймні з одним непробільним символом, збережений без змін.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct NonBlank(String);

impl NonBlank {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Deref for NonBlank {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<NonBlank> for String {
    fn from(value: NonBlank) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for NonBlank {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.trim().is_empty() {
            return Err(D::Error::custom("string must not be blank"));
        }
        Ok(Self(value))
    }
}

impl JsonSchema for NonBlank {
    fn schema_name() -> Cow<'static, str> {
        "NonBlank".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": "string", "pattern": r"\S"})
    }
}

/// Any JSON value except `null`; a missing field is also rejected.
/// Будь-яке значення JSON, окрім `null`; відсутнє поле також відхиляється.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NonNull(Value);

impl NonNull {
    pub fn into_inner(self) -> Value {
        self.0
    }
}

impl Deref for NonNull {
    type Target = Value;

    fn deref(&self) -> &Value {
        &self.0
    }
}

impl From<NonNull> for Value {
    fn from(value: NonNull) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for NonNull {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::Null => Err(D::Error::custom("value must not be null")),
            value => Ok(Self(value)),
        }
    }
}

impl JsonSchema for NonNull {
    fn schema_name() -> Cow<'static, str> {
        "NonNull".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"not": {"type": "null"}})
    }
}

/// For `#[serde(default, deserialize_with = "keep_null")]` on `Option<Value>`: a present
/// `null` becomes `Some(Value::Null)` instead of `None`, so it can be forwarded unchanged.
pub fn keep_null<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests;
