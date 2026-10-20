#!/usr/bin/env bash
# Builds libjson_rpc_2_ffi for every mobile target this repository supports
# and regenerates the Kotlin/Swift bindings:
#   - Android: a .so per ABI (arm64-v8a, armeabi-v7a, x86, x86_64), copied
#     into ../../JSON_RPC_2_KMP/jsonrpc-client/src/main/jniLibs/
#   - iOS: a JsonRpc2FFI.xcframework (device + simulator slices), copied
#     into ../../JSON_RPC_2_KMP/ios/JsonRpc2Client/Frameworks/
#
# Requires: rustup targets for the triples below, `cargo-ndk`
# (`cargo install cargo-ndk`), an Android NDK (set ANDROID_NDK_HOME or edit
# the default below), and Xcode command line tools.
set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$CRATE_DIR/.."
WORKSPACE_DIR="$PWD"
KMP_DIR="$CRATE_DIR/../../JSON_RPC_2_KMP"
ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-$HOME/Library/Android/sdk/ndk/27.0.12077973}"
export ANDROID_NDK_HOME

BINDINGS_DIR="$CRATE_DIR/bindings"
JNI_LIBS_DIR="$KMP_DIR/jsonrpc-client/src/main/jniLibs"
XCFRAMEWORK_DIR="$KMP_DIR/ios/JsonRpc2Client/Frameworks"
SWIFT_SOURCES_DIR="$KMP_DIR/ios/JsonRpc2Client/Sources/JsonRpc2Client"
KOTLIN_PACKAGE_DIR="$KMP_DIR/jsonrpc-client/src/main/kotlin/uniffi/json_rpc_2_ffi"

echo "==> Building host binary (for uniffi-bindgen)"
cargo build -p json-rpc-2-ffi

echo "==> Building target libsrt dependencies"
bash "$KMP_DIR/tools/build-mobile-srt.sh"

SRT_REVISION=899348d8318eb9a3c5a5b6ec43c4a1114288773a
SRT_SOURCE_DIR="$WORKSPACE_DIR/target/native/srt/$SRT_REVISION/source"

echo "==> Building Android .so for arm64-v8a, armeabi-v7a, x86, x86_64"
rm -rf "$BINDINGS_DIR/android-libs"
for abi in arm64-v8a armeabi-v7a x86 x86_64; do
    srt_prefix="$SRT_SOURCE_DIR/scripts/build-android/$abi"
    pkg_config_dir="$srt_prefix/lib/pkgconfig"
    env -u PKG_CONFIG_ALL_STATIC -u SRT_STATIC \
        PKG_CONFIG_ALLOW_CROSS=1 \
        PKG_CONFIG_PATH="$pkg_config_dir" \
        PKG_CONFIG_LIBDIR="$pkg_config_dir" \
        SRT_DYNAMIC=1 \
        cargo ndk -t "$abi" -o "$BINDINGS_DIR/android-libs" \
            build -p json-rpc-2-ffi --release

    native_dir="$SRT_SOURCE_DIR/scripts/build-android/prebuilt/$abi"
    for library in libsrt.so libmbedtls.so libmbedx509.so libmbedcrypto.so; do
        if [[ ! -f "$native_dir/$library" ]]; then
            echo "Missing Android SRT dependency: $native_dir/$library" >&2
            exit 1
        fi
        cp "$native_dir/$library" "$BINDINGS_DIR/android-libs/$abi/"
    done
done

echo "==> Building iOS static libs (device + simulator)"
for target in aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios; do
    srt_prefix="$WORKSPACE_DIR/target/native/srt/$SRT_REVISION/ios/$target"
    pkg_config_dir="$srt_prefix/lib/pkgconfig"
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_ALL_STATIC=1 \
    PKG_CONFIG_PATH="$pkg_config_dir" \
    PKG_CONFIG_LIBDIR="$pkg_config_dir" \
        cargo build -p json-rpc-2-ffi --release --target "$target"
done

echo "==> Generating Kotlin bindings"
rm -rf "$BINDINGS_DIR/kotlin"
cargo run -p json-rpc-2-ffi --features uniffi-bindgen --bin uniffi-bindgen -- \
    generate --library target/debug/libjson_rpc_2_ffi.dylib \
    --config "$CRATE_DIR/uniffi.toml" \
    --language kotlin --out-dir "$BINDINGS_DIR/kotlin"

echo "==> Generating Swift bindings"
rm -rf "$BINDINGS_DIR/swift"
cargo run -p json-rpc-2-ffi --features uniffi-bindgen --bin uniffi-bindgen -- \
    generate --library target/debug/libjson_rpc_2_ffi.dylib \
    --language swift --out-dir "$BINDINGS_DIR/swift"

echo "==> Assembling iOS XCFramework"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
mkdir -p "$WORK_DIR/device-headers" "$WORK_DIR/sim-headers"
cp "$BINDINGS_DIR/swift/json_rpc_2_ffiFFI.h" "$WORK_DIR/device-headers/"
cp "$BINDINGS_DIR/swift/json_rpc_2_ffiFFI.h" "$WORK_DIR/sim-headers/"
cp "$BINDINGS_DIR/swift/json_rpc_2_ffiFFI.modulemap" "$WORK_DIR/device-headers/module.modulemap"
cp "$BINDINGS_DIR/swift/json_rpc_2_ffiFFI.modulemap" "$WORK_DIR/sim-headers/module.modulemap"
lipo -create \
    target/aarch64-apple-ios-sim/release/libjson_rpc_2_ffi.a \
    target/x86_64-apple-ios/release/libjson_rpc_2_ffi.a \
    -output "$WORK_DIR/libjson_rpc_2_ffi_sim.a"

rm -rf "$XCFRAMEWORK_DIR/JsonRpc2FFI.xcframework"
mkdir -p "$XCFRAMEWORK_DIR"
xcodebuild -create-xcframework \
    -library target/aarch64-apple-ios/release/libjson_rpc_2_ffi.a -headers "$WORK_DIR/device-headers" \
    -library "$WORK_DIR/libjson_rpc_2_ffi_sim.a" -headers "$WORK_DIR/sim-headers" \
    -output "$XCFRAMEWORK_DIR/JsonRpc2FFI.xcframework"

echo "==> Copying outputs into the KMP project"
mkdir -p "$KOTLIN_PACKAGE_DIR"
cp "$BINDINGS_DIR/kotlin/uniffi/json_rpc_2_ffi/json_rpc_2_ffi.kt" "$KOTLIN_PACKAGE_DIR/"

for abi in arm64-v8a armeabi-v7a x86 x86_64; do
    mkdir -p "$JNI_LIBS_DIR/$abi"
    cp "$BINDINGS_DIR/android-libs/$abi/libjson_rpc_2_ffi.so" "$JNI_LIBS_DIR/$abi/"
done

cp "$BINDINGS_DIR/swift/json_rpc_2_ffi.swift" "$SWIFT_SOURCES_DIR/"

echo "==> Done."
echo "    Android: $JNI_LIBS_DIR"
echo "    iOS:     $XCFRAMEWORK_DIR/JsonRpc2FFI.xcframework"
