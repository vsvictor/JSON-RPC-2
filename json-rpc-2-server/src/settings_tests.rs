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
