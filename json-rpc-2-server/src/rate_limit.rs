use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

use crate::{RATE_LIMIT_ERROR_CODE, RpcError};

#[derive(Clone, Copy, Debug)]
/// Token-bucket refill rate and capacity. Zero in either field disables limiting.
/// Швидкість поповнення та місткість token bucket. Нуль вимикає обмеження.
pub struct RateLimitConfig {
    pub per_minute: u32,
    pub burst: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            per_minute: 10,
            burst: 5,
        }
    }
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Independent token buckets keyed by caller-provided identity strings.
/// Незалежні token bucket за рядковими ключами, наданими викликачем.
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Missing/blank keys bypass limiting; keys are trimmed before lookup.
    /// Відсутні/порожні ключі не обмежуються; крайні пробіли ключів відкидаються.
    pub async fn check(&self, identity: Option<&str>) -> Result<(), RpcError> {
        let Some(identity) = identity.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(());
        };
        if self.config.per_minute == 0 || self.config.burst == 0 {
            return Ok(());
        }
        let now = Instant::now();
        let capacity = f64::from(self.config.burst);
        let refill = f64::from(self.config.per_minute) / 60.0;
        let mut buckets = self.buckets.lock().await;
        let bucket = buckets.entry(identity.to_owned()).or_insert(Bucket {
            tokens: capacity,
            last_refill: now,
        });
        let elapsed = now
            .saturating_duration_since(bucket.last_refill)
            .as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * refill).min(capacity);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            return Ok(());
        }
        let retry = Duration::from_secs_f64(((1.0 - bucket.tokens) / refill).max(0.001));
        Err(RpcError::new(
            RATE_LIMIT_ERROR_CODE,
            format!("Too many requests; retry after {}s", retry.as_secs().max(1)),
        ))
    }

    /// Evicts idle buckets only after they could have fully replenished.
    /// Видаляє неактивні bucket лише після можливого повного поповнення.
    pub async fn cleanup(&self, idle_for: Duration) -> usize {
        let now = Instant::now();
        // An idle bucket must be fully replenished before evicting it.
        let refill_time = if self.config.per_minute > 0 {
            Duration::from_secs_f64(
                f64::from(self.config.burst) * 60.0 / f64::from(self.config.per_minute),
            )
        } else {
            Duration::ZERO
        };
        let mut buckets = self.buckets.lock().await;
        let before = buckets.len();
        buckets.retain(|_, bucket| {
            now.saturating_duration_since(bucket.last_refill) < idle_for.max(refill_time)
        });
        before - buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn capacity_keys_and_error_contract() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_minute: 10,
            burst: 1,
        });
        limiter.check(Some(" key ")).await.unwrap();
        let error = limiter.check(Some("key")).await.unwrap_err();
        assert_eq!(error.code, RATE_LIMIT_ERROR_CODE);
        assert!(error.message.starts_with("Too many requests; retry after "));
        assert!(error.message.ends_with('s'));
        limiter.check(Some("other")).await.unwrap();
        limiter.check(None).await.unwrap();
        limiter.check(Some(" ")).await.unwrap();
    }

    #[tokio::test]
    async fn zero_disables_limiting() {
        for config in [
            RateLimitConfig {
                per_minute: 0,
                burst: 1,
            },
            RateLimitConfig {
                per_minute: 1,
                burst: 0,
            },
        ] {
            let limiter = RateLimiter::new(config);
            for _ in 0..3 {
                limiter.check(Some("key")).await.unwrap();
            }
            assert!(limiter.buckets.lock().await.is_empty());
        }
    }

    #[tokio::test]
    async fn refill_and_cleanup_preserve_limits() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_minute: 60,
            burst: 2,
        });
        limiter.check(Some("key")).await.unwrap();
        limiter.check(Some("key")).await.unwrap();
        assert_eq!(limiter.cleanup(Duration::ZERO).await, 0);
        {
            let mut buckets = limiter.buckets.lock().await;
            buckets.get_mut("key").unwrap().last_refill = Instant::now() - Duration::from_secs(3);
        }
        limiter.check(Some("key")).await.unwrap();
        limiter.check(Some("key")).await.unwrap();
        assert!(limiter.check(Some("key")).await.is_err());
        {
            let mut buckets = limiter.buckets.lock().await;
            buckets.get_mut("key").unwrap().last_refill = Instant::now() - Duration::from_secs(3);
        }
        assert_eq!(limiter.cleanup(Duration::from_secs(10)).await, 0);
        assert_eq!(limiter.cleanup(Duration::ZERO).await, 1);
    }

    #[tokio::test]
    async fn concurrent_checks_share_capacity() {
        let limiter = std::sync::Arc::new(RateLimiter::new(RateLimitConfig {
            per_minute: 1,
            burst: 2,
        }));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..10 {
            let limiter = limiter.clone();
            tasks.spawn(async move { limiter.check(Some("key")).await.is_ok() });
        }
        let mut accepted = 0;
        while let Some(result) = tasks.join_next().await {
            accepted += usize::from(result.unwrap());
        }
        assert_eq!(accepted, 2);
    }
}
