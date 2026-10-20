fn main() {
    if std::env::var_os("CARGO_FEATURE_SRT").is_some() {
        for variable in ["SRT_FORCE_VENDORED", "SRT_NO_PKG_CONFIG"] {
            println!("cargo:rerun-if-env-changed={variable}");
            assert!(
                std::env::var_os(variable).is_none(),
                "{variable} is unsupported: this adapter requires system libsrt with encryption; see SRT.md"
            );
        }
        pkg_config::Config::new()
            .atleast_version("1.5.0")
            .probe("srt")
            .expect("SRT requires system libsrt >= 1.5 with encryption enabled and pkg-config. Install libsrt-dev (Debian/Ubuntu) or brew install srt pkg-config (macOS); set PKG_CONFIG_PATH for nonstandard installations. See SRT.md.");

        if std::env::var("CARGO_CFG_TARGET_OS").is_ok_and(|target_os| target_os == "ios") {
            println!("cargo:rustc-link-lib=c++");
        }
    }
}
