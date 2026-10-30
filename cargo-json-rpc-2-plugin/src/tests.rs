use std::fs;

use tempfile::TempDir;

use super::*;

const ROOT_MANIFEST: &str = r#"[package]
name = "app"
version = "0.1.0"
edition = "2024"

[features]
default = []
tms = ["dep:tms-server-main"]

[dependencies]
tms-server-main = { path = "tms-server-main", version = "0.1.0", optional = true }
tokio.workspace = true

[workspace]
members = ["tms-server-main"]

[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.dependencies]
anyhow = "1"
json-rpc-2-server = { git = "https://example.invalid/JSON-RPC-2", rev = "abc" }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros"] }

[workspace.metadata.json-rpc-2-plugin]
crate-prefix = "tms-server-"
"#;

const MAIN: &str = "fn main() {
    #[allow(unused_mut)]
    let mut builder = make();
    #[cfg(feature = \"tms\")]
    {
        builder = builder.plugin_from_env::<tms_server_main::Config>()?;
    }
    builder.build().await?.run_until_signal().await
}
";

fn workspace(manifest: &str, main: Option<&str>, env: Option<&str>) -> TempDir {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("Cargo.toml"), manifest).unwrap();
    if let Some(main) = main {
        fs::create_dir(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("src/main.rs"), main).unwrap();
    }
    if let Some(env) = env {
        fs::write(dir.path().join(".env.example"), env).unwrap();
    }
    dir
}

fn options(dir: &TempDir, name: &str) -> Options {
    Options {
        name: name.to_owned(),
        workspace: dir.path().to_owned(),
        crate_prefix: None,
        no_wire: false,
    }
}

fn read(dir: &TempDir, path: &str) -> String {
    fs::read_to_string(dir.path().join(path)).unwrap()
}

#[test]
fn names_are_derived_from_any_spelling() {
    let names = Names::new(" Fleet-Stats ", "tms-server-").unwrap();
    assert_eq!(
        names,
        Names {
            snake: "fleet_stats".into(),
            kebab: "fleet-stats".into(),
            krate: "tms-server-fleet-stats".into(),
            module: "tms_server_fleet_stats".into(),
            pascal: "FleetStats".into(),
            env_prefix: "FLEET_STATS_".into(),
        }
    );
    assert_eq!(Names::new("hello", "").unwrap().krate, "hello");
    for bad in ["", "1x", "a__b", "x_", "h llo", "héllo"] {
        assert!(Names::new(bad, "").is_err(), "{bad}");
    }
    assert!(Names::new("ok", "Bad Prefix").is_err());
}

#[test]
fn generates_crate_and_wires_workspace() {
    let dir = workspace(ROOT_MANIFEST, Some(MAIN), Some("A=1\n"));
    let (names, report) = generate(&options(&dir, "fleet-stats")).unwrap();
    assert_eq!(names.krate, "tms-server-fleet-stats");
    assert_eq!(report.feature.as_deref(), Some("fleet-stats"));
    assert_eq!(report.created.len(), 5);
    assert!(report.hints.is_empty());

    let plugin = read(&dir, "tms-server-fleet-stats/Cargo.toml");
    for line in [
        "name = \"tms-server-fleet-stats\"",
        "version.workspace = true",
        "anyhow.workspace = true",
        "json-rpc-2-server.workspace = true",
        "serde.workspace = true",
        "serde_json = \"1\"",
        "tracing = \"0.1\"",
        "tokio.workspace = true",
    ] {
        assert!(plugin.contains(line), "{line}\n{plugin}");
    }
    let lib = read(&dir, "tms-server-fleet-stats/src/lib.rs");
    assert!(lib.contains("pub mod service;"));
    assert!(lib.contains("#[settings(prefix = \"FLEET_STATS_\")]"));
    assert!(lib.contains("const NAME: &'static str = \"fleet_stats\";"));
    assert!(!lib.contains("__"));
    let service = read(&dir, "tms-server-fleet-stats/src/service.rs");
    assert!(service.contains("#[rpc(name = \"fleet_stats\")]"));
    assert!(service.contains("pub struct FleetStatsService"));
    assert!(
        dir.path()
            .join("tms-server-fleet-stats/src/service/tests.rs")
            .is_file()
    );

    let manifest: DocumentMut = read(&dir, "Cargo.toml").parse().unwrap();
    let members: Vec<_> = manifest["workspace"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|member| member.as_str())
        .collect();
    assert_eq!(members, ["tms-server-main", "tms-server-fleet-stats"]);
    assert_eq!(
        manifest["features"]["fleet-stats"]
            .as_array()
            .unwrap()
            .get(0)
            .unwrap()
            .as_str(),
        Some("dep:tms-server-fleet-stats")
    );
    let dependency = manifest["dependencies"]["tms-server-fleet-stats"]
        .as_inline_table()
        .unwrap();
    assert_eq!(dependency.get("optional").unwrap().as_bool(), Some(true));
    assert!(read(&dir, "Cargo.toml").contains("tms = [\"dep:tms-server-main\"]"));

    let main = read(&dir, "src/main.rs");
    let block = "    #[cfg(feature = \"fleet-stats\")]\n    {\n        builder = builder.plugin_from_env::<tms_server_fleet_stats::FleetStatsConfig>()?;\n    }\n    builder.build()";
    assert!(main.contains(block), "{main}");

    let env = read(&dir, ".env.example");
    assert!(env.starts_with("A=1\n\n# ---- tms-server-fleet-stats ----\n"));
    assert!(env.contains("FLEET_STATS_RESET_SECS=3600\n"));
}

#[test]
fn refuses_existing_plugin_and_reserved_feature() {
    let dir = workspace(ROOT_MANIFEST, Some(MAIN), None);
    generate(&options(&dir, "hello")).unwrap();
    let error = generate(&options(&dir, "hello")).unwrap_err().to_string();
    assert!(error.contains("already exists"), "{error}");

    let mut clash = options(&dir, "tms");
    clash.crate_prefix = Some("other-".into());
    let before = read(&dir, "Cargo.toml");
    let error = generate(&clash).unwrap_err().to_string();
    assert!(error.contains("feature `tms`"), "{error}");
    assert_eq!(read(&dir, "Cargo.toml"), before);
    assert!(!dir.path().join("other-tms").exists());
}

#[test]
fn writes_nothing_when_main_has_no_anchor() {
    let dir = workspace(ROOT_MANIFEST, Some("fn main() {}\n"), Some("A=1\n"));
    let error = generate(&options(&dir, "hello")).unwrap_err().to_string();
    assert!(error.contains("builder.build()"), "{error}");
    assert_eq!(read(&dir, "Cargo.toml"), ROOT_MANIFEST);
    assert_eq!(read(&dir, ".env.example"), "A=1\n");
    assert!(!dir.path().join("tms-server-hello").exists());
}

#[test]
fn virtual_workspace_gets_member_and_explicit_dependencies() {
    let dir = workspace("[workspace]\nmembers = []\n", None, None);
    let (names, report) = generate(&options(&dir, "hello")).unwrap();
    assert_eq!(names.krate, "hello");
    assert_eq!(report.feature, None);
    assert_eq!(report.hints.len(), 2);
    let plugin = read(&dir, "hello/Cargo.toml");
    assert!(plugin.contains("version = \"0.1.0\""));
    assert!(plugin.contains(&format!(
        "json-rpc-2-server = {{ git = \"{JSON_RPC_2_GIT}\""
    )));
    assert!(plugin.contains("tokio = { version = \"1\", features = [\"macros\", \"rt\"] }"));
    assert!(read(&dir, "Cargo.toml").contains("members = [\"hello\"]"));
}

#[test]
fn no_wire_only_adds_member() {
    let dir = workspace(ROOT_MANIFEST, Some(MAIN), None);
    let mut options = options(&dir, "hello");
    options.no_wire = true;
    let (_, report) = generate(&options).unwrap();
    assert_eq!(report.feature, None);
    assert_eq!(read(&dir, "src/main.rs"), MAIN);
    assert!(!read(&dir, "Cargo.toml").contains("hello = "));
}

#[test]
fn rejects_non_workspace_manifest() {
    let dir = workspace("[package]\nname = \"x\"\n", None, None);
    let error = generate(&options(&dir, "hello")).unwrap_err().to_string();
    assert!(error.contains("no [workspace]"), "{error}");
}
