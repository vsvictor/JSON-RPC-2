//! Shared environment configuration for servers and plugins.
//!
//! Спільна конфігурація середовища для серверів і плагінів.
//!
//! ```
//! use json_rpc_2_server::{Settings, process_env};
//!
//! let values = process_env();
//! let settings = Settings::new(&values);
//! let pool_size = settings.number("POOL_SIZE", 8usize)?;
//! # Ok::<(), anyhow::Error>(())
//! ```

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, Result, anyhow, ensure};

/// Snapshot of Unicode process environment entries.
///
/// Does not load files or mutate the process environment.
pub fn process_env() -> HashMap<String, String> {
    std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect()
}

/// Read-only configuration view without copying the source map.
///
/// Blank values count as unset. Errors name keys but never include values.
pub struct Settings<'a>(&'a HashMap<String, String>);

impl<'a> Settings<'a> {
    pub fn new(values: &'a HashMap<String, String>) -> Self {
        Self(values)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).filter(|v| !v.trim().is_empty()).cloned()
    }

    /// Like `get`, but rejects an explicitly configured blank value.
    pub fn nonempty(&self, key: &str) -> Result<Option<String>> {
        if let Some(value) = self.0.get(key) {
            ensure!(!value.trim().is_empty(), "{key} must not be empty");
        }
        Ok(self.get(key))
    }

    pub fn required(&self, key: &str) -> Result<String> {
        self.get(key).with_context(|| format!("{key} is required"))
    }

    pub fn number<T: FromStr>(&self, key: &str, default: T) -> Result<T> {
        match self.get(key) {
            Some(value) => value
                .trim()
                .parse()
                .map_err(|_| anyhow!("invalid numeric value for {key}")),
            None => Ok(default),
        }
    }

    /// A port in `1..=65535`; blank values use the default.
    pub fn port(&self, key: &str, default: u16) -> Result<u16> {
        let port = self.number(key, default)?;
        ensure!(port > 0, "{key} must be positive");
        Ok(port)
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
