//! Procedural macros re-exported by `json-rpc-2-server`; use them from there.
//!
//! Процедурні макроси, які реекспортує `json-rpc-2-server`; використовуйте їх звідти.

use proc_macro::TokenStream;

mod rpc_api;
mod rpc_methods;
mod settings;

/// Contract-first API: annotate params / result types, implement the generated trait elsewhere.
///
/// API від контракту: позначте типи параметрів / результатів, а згенерований трейт
/// реалізуйте в іншому файлі.
///
/// Put on an inline module. For every `#[params(...)]` type it adds
/// `#[derive(Deserialize, JsonSchema)]`, for every `#[result]` type `#[derive(Serialize)]`;
/// structs with named fields also get `#[serde(rename_all = "camelCase")]` unless they set
/// `rename_all` themselves. It then generates a trait (default `Api`) with one
/// `async fn handler(&self, params: P) -> RpcResult<R>` per method and a provided
/// `register_rpc(self: Arc<Self>, server)` (plus `limiter: &Arc<RateLimiter>` if any method
/// has `limit`). Forgetting a method is a compile error in the implementing file.
/// The crate must depend on `serde` and `schemars`.
///
/// Ставиться на вбудований модуль. Типам `#[params(...)]` додає
/// `#[derive(Deserialize, JsonSchema)]`, типам `#[result]` — `#[derive(Serialize)]`;
/// структури з іменованими полями також отримують `#[serde(rename_all = "camelCase")]`,
/// якщо самі не задають `rename_all`. Далі генерує трейт (за замовчуванням `Api`) з одним
/// `async fn handler(&self, params: P) -> RpcResult<R>` на метод і готовим
/// `register_rpc(self: Arc<Self>, server)` (плюс `limiter: &Arc<RateLimiter>`, якщо в
/// якогось методу є `limit`). Пропущений метод — помилка компіляції у файлі реалізації.
/// Крейт має залежати від `serde` і `schemars`.
///
/// | Attribute / Атрибут | Meaning / Значення |
/// |---|---|
/// | `#[rpc_api(alias_prefix = "rpc.")]` | adds `rpc.<method>` for every method / додає `rpc.<method>` для кожного методу |
/// | `#[rpc_api(trait_name = "HelloApi")]` | trait name, default `Api` / ім'я трейту, типово `Api` |
/// | `#[params(method = "getShift", result = Shift)]` | method name and result type, required / ім'я методу й тип результату, обов'язкові |
/// | `#[params(..., handler = "shift")]` | trait fn name, default snake_case of the method (`get_shift`) / ім'я fn у трейті, типово snake_case методу |
/// | `#[params(..., alias = "x.y")]` | extra alias, repeatable / додатковий псевдонім, можна кілька |
/// | `#[params(..., limit = "phone")]` | rate limit by the raw `phone` field / ліміт за сирим полем `phone` |
/// | `#[result]` | result type / тип результату |
///
/// ```ignore
/// // src/api.rs — contracts only / лише контракти
/// #[json_rpc_2_server::rpc_api(alias_prefix = "rpc.")]
/// pub mod api {
///     use json_rpc_2_server::NonBlank;
///
///     #[params(method = "hello", result = HelloResult)]
///     pub struct HelloParams { pub name: NonBlank }
///
///     #[result]
///     pub struct HelloResult { pub message: String }
/// }
///
/// // src/logic.rs — application logic only / лише прикладна логіка
/// impl api::Api for HelloService {
///     async fn hello(&self, params: api::HelloParams) -> RpcResult<api::HelloResult> { /* ... */ }
/// }
///
/// Arc::new(HelloService::new()).register_rpc(&server)?; // hello, rpc.hello
/// ```
#[proc_macro_attribute]
pub fn rpc_api(args: TokenStream, input: TokenStream) -> TokenStream {
    rpc_api::expand(args.into(), input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Generates `register_rpc` for the `#[rpc]` methods of an inherent `impl` block.
///
/// Генерує `register_rpc` для методів `#[rpc]` у власному блоці `impl`.
///
/// Each `#[rpc]` method must be `async fn name(&self, params: P) -> RpcResult<R>` where
/// `P: Deserialize + JsonSchema` and `R: Serialize`; it is registered with
/// `register_typed` (or `register_limited` when `limit` is set). Other methods are left
/// untouched.
///
/// Кожен метод `#[rpc]` має виглядати як `async fn name(&self, params: P) -> RpcResult<R>`;
/// він реєструється через `register_typed` (або `register_limited`, якщо задано `limit`).
///
/// | Attribute / Атрибут | Meaning / Значення |
/// |---|---|
/// | `#[rpc_methods(alias_prefix = "rpc.")]` | adds `rpc.<name>` for every method / додає `rpc.<name>` для кожного методу |
/// | `#[rpc]` | method name = function name / ім'я методу = ім'я функції |
/// | `#[rpc(name = "getShift")]` | explicit method name / явне ім'я методу |
/// | `#[rpc(alias = "x.y")]` | extra alias, repeatable / додатковий псевдонім, можна кілька |
/// | `#[rpc(limit = "phone")]` | rate limit by the raw `phone` field before validation / ліміт за сирим полем `phone` до валідації |
///
/// Generated: `pub fn register_rpc(self: Arc<Self>, server: &RpcServer) -> Result<(), RegistrationError>`;
/// if any method has `limit`, it takes an extra `limiter: &Arc<RateLimiter>`.
/// Use one `#[rpc_methods]` block per type.
///
/// ```ignore
/// #[rpc_methods(alias_prefix = "rpc.")]
/// impl HelloService {
///     #[rpc]
///     async fn hello(&self, params: HelloParams) -> RpcResult<HelloResult> { /* ... */ }
/// }
///
/// Arc::new(HelloService::new()).register_rpc(&server)?; // hello, rpc.hello
/// ```
#[proc_macro_attribute]
pub fn rpc_methods(args: TokenStream, input: TokenStream) -> TokenStream {
    rpc_methods::expand(args.into(), input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives `FromSettings` from `#[settings(...)]` / `#[setting(...)]` attributes.
///
/// Виводить `FromSettings` з атрибутів `#[settings(...)]` / `#[setting(...)]`.
///
/// The key of a field is `prefix + FIELD_NAME` unless `key` is given. Blank values count
/// as unset; errors name the key but never the value.
///
/// Ключ поля — `prefix + FIELD_NAME`, якщо не задано `key`. Порожні значення вважаються
/// незаданими; помилки називають ключ, але ніколи не значення.
///
/// | Field / Поле | Read as / Як читається |
/// |---|---|
/// | `String` | required, kept as is / обов'язкове, без змін |
/// | `String` + `default = "x"` | optional with default / необов'язкове зі значенням за замовчуванням |
/// | `Option<T>` | `None` when unset / `None`, якщо не задано |
/// | other `T: FromStr` / інший `T: FromStr` | trimmed and parsed; required unless `default` / обрізане й розібране; обов'язкове без `default` |
/// | `Duration` + `secs` / `millis` + `default` | whole seconds / milliseconds, 0 → 1 / цілі секунди / мілісекунди, 0 → 1 |
/// | `port` + `default` | port `1..=65535` / порт `1..=65535` |
/// | `min = 1` | raises smaller values / піднімає менші значення |
/// | `nested` | `T::from_settings(settings)` |
/// | `with = expr` | `expr(settings)?`, e.g. a fn or closure / функція чи замикання |
///
/// ```ignore
/// #[derive(FromSettings)]
/// #[settings(prefix = "HELLO_")]
/// struct HelloConfig {
///     #[setting(default = "Hello")]
///     greeting: String,                 // HELLO_GREETING
///     #[setting(secs, default = 3600)]
///     reset: Duration,                  // HELLO_RESET
///     #[setting(key = "PG_POOL_SIZE", default = 8, min = 1)]
///     pool_size: usize,
/// }
/// ```
#[proc_macro_derive(FromSettings, attributes(settings, setting))]
pub fn derive_from_settings(input: TokenStream) -> TokenStream {
    settings::expand(syn::parse_macro_input!(input))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
