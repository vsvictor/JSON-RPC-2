//! `cargo json-rpc-2-plugin new <name>`: scaffolds a json-rpc-2-server plugin.

use std::{env, path::PathBuf, process::Command};

use anyhow::{Context, Result, bail};
use cargo_json_rpc_2_plugin::{Options, display, generate};

const USAGE: &str = "\
Scaffold a json-rpc-2-server plugin crate / Створити крейт плагіна json-rpc-2-server

USAGE:
    cargo json-rpc-2-plugin new <name> [OPTIONS]

OPTIONS:
    --path <dir>            workspace root (default: current directory)
    --crate-prefix <text>   crate name prefix (default: [workspace.metadata.json-rpc-2-plugin] crate-prefix)
    --no-wire               do not add a feature / src/main.rs block
    --api                   contract-first: src/api.rs (#[rpc_api] params/results) + src/logic.rs
    -h, --help              print help";

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args: Vec<String> = env::args().skip(1).collect();
    // `cargo json-rpc-2-plugin …` passes the subcommand name first.
    if args.first().map(String::as_str) == Some("json-rpc-2-plugin") {
        args.remove(0);
    }
    if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{USAGE}");
        return Ok(());
    }
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("new") => {}
        Some(other) => bail!("unknown command `{other}`\n\n{USAGE}"),
        None => unreachable!(),
    }

    let mut name = None;
    let mut workspace = env::current_dir()?;
    let mut crate_prefix = None;
    let mut no_wire = false;
    let mut api = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => workspace = PathBuf::from(args.next().context("--path needs a value")?),
            "--crate-prefix" => {
                crate_prefix = Some(args.next().context("--crate-prefix needs a value")?)
            }
            "--no-wire" => no_wire = true,
            "--api" => api = true,
            flag if flag.starts_with('-') => bail!("unknown option `{flag}`\n\n{USAGE}"),
            _ if name.is_none() => name = Some(arg),
            _ => bail!("unexpected argument `{arg}`"),
        }
    }
    let options = Options {
        name: name.context("missing plugin name")?,
        workspace,
        crate_prefix,
        no_wire,
        api,
    };
    let (names, report) = generate(&options)?;

    // Best effort: templates are formatted already, long names may need rewrapping.
    let _ = Command::new("cargo")
        .args(["fmt", "-p", &names.krate])
        .current_dir(&options.workspace)
        .status();

    let root = &options.workspace;
    println!("created {}", names.krate);
    for path in &report.created {
        println!("  + {}", display(root, path));
    }
    for path in &report.changed {
        println!("  ~ {}", display(root, path));
    }
    for hint in &report.hints {
        println!("note: {hint}");
    }
    println!("next: cargo test -p {}", names.krate);
    if let Some(feature) = report.feature {
        println!("      cargo build --features {feature}");
    }
    Ok(())
}
