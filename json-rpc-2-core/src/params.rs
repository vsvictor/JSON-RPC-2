//! Generic parameter validation using the standard invalid-params error.
//! Загальна перевірка параметрів зі стандартною помилкою invalid params.

use serde_json::{Map, Value};

use crate::RpcError;

/// Consumes an object without copying its fields.
/// Приймає об'єкт без копіювання його полів.
pub fn object(params: Value) -> Result<Map<String, Value>, RpcError> {
    match params {
        Value::Object(map) => Ok(map),
        _ => Err(RpcError::invalid_params()),
    }
}

/// Borrows a required nonblank string, preserving its original whitespace.
/// Позичає обов'язковий непорожній рядок, зберігаючи початкові пробіли.
pub fn required_str<'a>(map: &'a Map<String, Value>, field: &str) -> Result<&'a str, RpcError> {
    map.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(RpcError::invalid_params)
}

/// Returns an owned copy of a required non-null value.
/// Повертає власну копію обов'язкового значення, відмінного від null.
pub fn required_value(map: &Map<String, Value>, field: &str) -> Result<Value, RpcError> {
    map.get(field)
        .filter(|value| !value.is_null())
        .cloned()
        .ok_or_else(RpcError::invalid_params)
}

#[cfg(test)]
mod tests;
