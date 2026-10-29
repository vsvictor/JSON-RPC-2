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

#[test]
fn config_from_prefixed_settings() {
    let values = std::collections::HashMap::from([
        ("AUTH_RATE_PER_MINUTE".to_owned(), "20".to_owned()),
        ("BAD_BURST".to_owned(), "x".to_owned()),
    ]);
    let settings = Settings::new(&values);
    let config = RateLimitConfig::from_prefix(&settings, "AUTH_RATE").unwrap();
    assert_eq!((config.per_minute, config.burst), (20, 5));
    assert!(RateLimitConfig::from_prefix(&settings, "BAD").is_err());
}
