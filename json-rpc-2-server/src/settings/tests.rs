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

#[test]
fn from_settings_provides_from_values() {
    struct Pool(usize);
    impl FromSettings for Pool {
        fn from_settings(settings: &Settings<'_>) -> Result<Self> {
            Ok(Self(settings.number("POOL_SIZE", 8)?))
        }
    }
    assert_eq!(
        Pool::from_values(&values(&[("POOL_SIZE", "3")])).unwrap().0,
        3
    );
    assert!(Pool::from_values(&values(&[("POOL_SIZE", "x")])).is_err());
}

#[test]
fn durations_default_and_never_zero() {
    let values = values(&[("ZERO", "0"), ("MS", "250"), ("BAD", "x")]);
    let settings = Settings::new(&values);
    assert_eq!(
        settings.duration_secs("MISSING", 5).unwrap(),
        Duration::from_secs(5)
    );
    assert_eq!(
        settings.duration_secs("ZERO", 5).unwrap(),
        Duration::from_secs(1)
    );
    assert_eq!(
        settings.duration_millis("MS", 1).unwrap(),
        Duration::from_millis(250)
    );
    assert_eq!(
        settings.duration_millis("ZERO", 9).unwrap(),
        Duration::from_millis(1)
    );
    assert!(settings.duration_secs("BAD", 1).is_err());
}

#[test]
fn parse_helpers_trim_and_hide_values() {
    let values = values(&[("FLAG", " true "), ("BAD", "secret")]);
    let settings = Settings::new(&values);
    assert_eq!(settings.parse::<bool>("FLAG").unwrap(), Some(true));
    assert_eq!(settings.parse::<bool>("MISSING").unwrap(), None);
    assert!(settings.parse_or("MISSING", false).is_ok_and(|v| !v));
    assert!(settings.parse_required::<u8>("MISSING").is_err());
    let error = settings.parse::<u8>("BAD").unwrap_err().to_string();
    assert!(error.contains("BAD") && !error.contains("secret"));
}

mod derive {
    use std::{collections::HashMap, time::Duration};

    use crate::{FromSettings, Settings};

    #[derive(Debug, PartialEq)]
    struct Pair(u8, u8);

    #[derive(FromSettings)]
    #[settings(prefix = "DEMO_")]
    struct Demo {
        name: String,
        #[setting(default = "guest")]
        user: String,
        note: Option<String>,
        #[setting(default = 8, min = 1)]
        pool: usize,
        retries: Option<u32>,
        #[setting(key = "SHARED_FLAG", default = false)]
        flag: bool,
        #[setting(secs, default = 5)]
        timeout: Duration,
        #[setting(millis, default = 300)]
        backoff: Duration,
        #[setting(port, default = 5432)]
        port: u16,
        #[setting(with = |s: &Settings<'_>| Ok::<_, anyhow::Error>(Pair(s.number("A", 1)?, 2)))]
        pair: Pair,
        #[setting(nested)]
        inner: Inner,
    }

    #[derive(FromSettings)]
    struct Inner {
        #[setting(key = "INNER")]
        value: u64,
    }

    fn values(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect()
    }

    #[test]
    fn defaults_keys_and_conversions() {
        let demo = Demo::from_values(&values(&[("DEMO_NAME", " n "), ("INNER", "9")])).unwrap();
        assert_eq!(demo.name, " n ");
        assert_eq!(demo.user, "guest");
        assert_eq!(demo.note, None);
        assert_eq!(demo.pool, 8);
        assert_eq!(demo.retries, None);
        assert!(!demo.flag);
        assert_eq!(demo.timeout, Duration::from_secs(5));
        assert_eq!(demo.backoff, Duration::from_millis(300));
        assert_eq!(demo.port, 5432);
        assert_eq!(demo.pair, Pair(1, 2));
        assert_eq!(demo.inner.value, 9);

        let demo = Demo::from_values(&values(&[
            ("DEMO_NAME", "n"),
            ("DEMO_USER", "u"),
            ("DEMO_NOTE", "x"),
            ("DEMO_POOL", "0"),
            ("DEMO_RETRIES", "3"),
            ("SHARED_FLAG", "true"),
            ("DEMO_TIMEOUT", "0"),
            ("DEMO_PORT", "1"),
            ("A", "7"),
            ("INNER", "1"),
        ]))
        .unwrap();
        assert_eq!(
            (demo.user.as_str(), demo.note.as_deref(), demo.pool),
            ("u", Some("x"), 1)
        );
        assert_eq!((demo.retries, demo.flag, demo.port), (Some(3), true, 1));
        assert_eq!(demo.timeout, Duration::from_secs(1));
        assert_eq!(demo.pair, Pair(7, 2));
    }

    #[test]
    fn required_and_invalid_values_name_the_key() {
        let error = Demo::from_values(&values(&[("INNER", "1")])).err().unwrap();
        assert!(error.to_string().contains("DEMO_NAME is required"));
        let error = Demo::from_values(&values(&[("DEMO_NAME", "n")]))
            .err()
            .unwrap();
        assert!(error.to_string().contains("INNER is required"));
        let error = Demo::from_values(&values(&[
            ("DEMO_NAME", "n"),
            ("INNER", "1"),
            ("DEMO_RETRIES", "many"),
        ]))
        .err()
        .unwrap();
        assert!(error.to_string().contains("DEMO_RETRIES") && !error.to_string().contains("many"));
    }
}
