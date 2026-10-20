//! Thin entry point for the `uniffi-bindgen` CLI, used to generate the
//! Kotlin (JVM/Android, via JNA) and Swift language bindings from the
//! `#[uniffi::export]` annotations in `src/lib.rs`.
//!
//! Run with, e.g.:
//!   cargo run --features uniffi-bindgen --bin uniffi-bindgen -- generate \
//!       --library target/debug/libjson_rpc_2_ffi.dylib \
//!       --language kotlin --out-dir bindings/kotlin

fn main() {
    uniffi::uniffi_bindgen_main()
}
