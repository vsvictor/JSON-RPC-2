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
mod tests {
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect()
    }

    #[test]
    fn blank_is_unset_and_defaults_apply() {
        let values = values(&[("A", "  "), ("B", " 7 "), ("S", " value ")]);
        let settings = Settings::new(&values);
        assert!(settings.get("A").is_none());
        assert!(settings.required("A").is_err());
        assert_eq!(settings.number("A", 3u32).unwrap(), 3);
        assert_eq!(settings.number("B", 3u32).unwrap(), 7);
        assert_eq!(settings.required("S").unwrap(), " value ");
    }

    #[test]
    fn errors_do_not_leak_values() {
        let values = values(&[("N", "secret"), ("P", "0"), ("BIG", "65536")]);
        let settings = Settings::new(&values);
        let error = settings.number("N", 1u32).unwrap_err().to_string();
        assert!(error.contains('N') && !error.contains("secret"));
        assert!(settings.port("P", 1).is_err());
        assert!(settings.port("BIG", 1).is_err());
        assert_eq!(settings.port("Q", 5432).unwrap(), 5432);
    }

    #[test]
    fn strict_access_rejects_blank_but_allows_absent_values() {
        let values = values(&[("A", " "), ("B", "value")]);
        let settings = Settings::new(&values);
        assert!(settings.nonempty("A").is_err());
        assert_eq!(settings.nonempty("B").unwrap().as_deref(), Some("value"));
        assert!(settings.nonempty("C").unwrap().is_none());
    }
}
