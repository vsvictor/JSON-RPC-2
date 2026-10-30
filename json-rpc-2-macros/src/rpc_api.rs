use std::collections::HashSet;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{
    Attribute, Error, Fields, Ident, Item, ItemMod, Result, Type, parse::Parser, parse_quote,
    spanned::Spanned,
};

use crate::rpc_methods::string_value;

struct Method {
    name: String,
    handler: Ident,
    params: Ident,
    result: Type,
    aliases: Vec<String>,
    limit: Option<String>,
}

pub fn expand(args: TokenStream, input: TokenStream) -> Result<TokenStream> {
    let mut alias_prefix = None::<String>;
    let mut trait_name = None::<Ident>;
    syn::meta::parser(|meta| {
        if meta.path.is_ident("alias_prefix") {
            alias_prefix = Some(string_value(&meta)?);
        } else if meta.path.is_ident("trait_name") {
            let value = meta.value()?.parse::<syn::LitStr>()?;
            trait_name = Some(value.parse()?);
        } else {
            return Err(meta.error("expected `alias_prefix = \"...\"` or `trait_name = \"...\"`"));
        }
        Ok(())
    })
    .parse2(args)?;
    let trait_name = trait_name.unwrap_or_else(|| Ident::new("Api", Span::call_site()));

    let mut module: ItemMod = syn::parse2(input)?;
    let Some((_, items)) = &mut module.content else {
        return Err(Error::new(
            module.span(),
            "#[rpc_api] needs an inline module: `mod api { ... }`",
        ));
    };

    let mut methods = Vec::<Method>::new();
    for item in items.iter_mut() {
        let (attrs, ident, is_struct) = match item {
            Item::Struct(item) => (
                &mut item.attrs,
                item.ident.clone(),
                matches!(item.fields, Fields::Named(_)),
            ),
            Item::Enum(item) => (&mut item.attrs, item.ident.clone(), false),
            _ => {
                if let Some(attrs) = item_attrs(item)
                    && let Some(attr) = attrs.iter().find(|attr| {
                        attr.path().is_ident("params") || attr.path().is_ident("result")
                    })
                {
                    return Err(Error::new(
                        attr.span(),
                        "#[params] / #[result] apply to structs and enums only",
                    ));
                }
                continue;
            }
        };
        let mut role = None::<(Attribute, bool)>;
        let mut kept = Vec::with_capacity(attrs.len());
        for attr in attrs.drain(..) {
            let is_params = attr.path().is_ident("params");
            if !is_params && !attr.path().is_ident("result") {
                kept.push(attr);
                continue;
            }
            if role.is_some() {
                return Err(Error::new(
                    attr.span(),
                    "use one #[params] or #[result] per type",
                ));
            }
            role = Some((attr, is_params));
        }
        *attrs = kept;
        let Some((attr, is_params)) = role else {
            continue;
        };
        let rename = is_struct && !has_serde_rename_all(attrs);
        if is_params {
            methods.push(parse_params(&attr, ident)?);
            attrs.insert(
                0,
                parse_quote!(#[derive(::serde::Deserialize, ::schemars::JsonSchema)]),
            );
        } else {
            if !matches!(attr.meta, syn::Meta::Path(_)) {
                return Err(Error::new(attr.span(), "#[result] takes no arguments"));
            }
            attrs.insert(0, parse_quote!(#[derive(::serde::Serialize)]));
        }
        if rename {
            attrs.insert(1, parse_quote!(#[serde(rename_all = "camelCase")]));
        }
    }
    if methods.is_empty() {
        return Err(Error::new(
            module.ident.span(),
            "#[rpc_api] found no #[params(method = \"...\", result = Type)] types",
        ));
    }
    let mut names = HashSet::new();
    let mut handlers = HashSet::new();
    for method in &methods {
        if !names.insert(method.name.as_str()) {
            return Err(Error::new(
                method.params.span(),
                format!("method `{}` is declared twice", method.name),
            ));
        }
        if !handlers.insert(method.handler.to_string()) {
            return Err(Error::new(
                method.params.span(),
                format!(
                    "handler `{}` is used twice; set `handler = \"...\"`",
                    method.handler
                ),
            ));
        }
    }

    let krate = quote!(::json_rpc_2_server);
    let signatures = methods.iter().map(|method| {
        let Method {
            name,
            handler,
            params,
            result,
            ..
        } = method;
        let doc = format!("Handles JSON-RPC `{name}`: [`{params}`] → result.");
        quote! {
            #[doc = #doc]
            fn #handler(
                &self,
                params: #params,
            ) -> impl ::std::future::Future<Output = #krate::RpcResult<#result>> + ::std::marker::Send;
        }
    });
    let registrations = methods.iter().map(|method| {
        let Method {
            name,
            handler,
            params,
            limit,
            ..
        } = method;
        let call = quote! {
            |this: ::std::sync::Arc<Self>, params: #params| async move { this.#handler(params).await }
        };
        match limit {
            Some(key) => quote! {
                server.register_limited(#name, ::std::sync::Arc::clone(&self), ::std::sync::Arc::clone(limiter), #key, #call)?;
            },
            None => quote! {
                server.register_typed(#name, ::std::sync::Arc::clone(&self), #call)?;
            },
        }
    });
    let aliases = methods.iter().flat_map(|method| {
        let target = &method.name;
        alias_prefix
            .as_ref()
            .map(|prefix| format!("{prefix}{target}"))
            .into_iter()
            .chain(method.aliases.iter().cloned())
            .map(move |alias| quote! { server.register_alias(#alias, #target)?; })
    });
    let limiter_arg = methods
        .iter()
        .any(|method| method.limit.is_some())
        .then(|| quote!(, limiter: &::std::sync::Arc<#krate::RateLimiter>));
    let list = methods
        .iter()
        .map(|method| format!("`{}`", method.name))
        .collect::<Vec<_>>()
        .join(", ");
    let trait_doc = format!(
        "JSON-RPC methods {list}; implement it in your logic file. Generated by `#[rpc_api]`."
    );
    let register_doc = format!("Registers {list} and their aliases on `server`.");
    let generated: Vec<Item> = vec![parse_quote! {
        #[doc = #trait_doc]
        pub trait #trait_name: ::std::marker::Send + ::std::marker::Sync + ::std::marker::Sized + 'static {
            #(#signatures)*

            #[doc = #register_doc]
            fn register_rpc(
                self: ::std::sync::Arc<Self>,
                server: &#krate::RpcServer
                #limiter_arg
            ) -> ::std::result::Result<(), #krate::RegistrationError> {
                #(#registrations)*
                #(#aliases)*
                ::std::result::Result::Ok(())
            }
        }
    }];
    if let Some((_, items)) = &mut module.content {
        items.extend(generated);
    }
    Ok(quote!(#module))
}

fn parse_params(attr: &Attribute, params: Ident) -> Result<Method> {
    let mut name = None::<String>;
    let mut result = None::<Type>;
    let mut handler = None::<Ident>;
    let mut aliases = Vec::new();
    let mut limit = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("method") {
            name = Some(string_value(&meta)?);
        } else if meta.path.is_ident("result") {
            result = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("handler") {
            handler = Some(meta.value()?.parse::<syn::LitStr>()?.parse()?);
        } else if meta.path.is_ident("alias") {
            aliases.push(string_value(&meta)?);
        } else if meta.path.is_ident("limit") {
            limit = Some(string_value(&meta)?);
        } else {
            return Err(meta.error("expected `method`, `result`, `handler`, `alias` or `limit`"));
        }
        Ok(())
    })?;
    let name = name.ok_or_else(|| Error::new(attr.span(), "#[params] needs `method = \"...\"`"))?;
    let result =
        result.ok_or_else(|| Error::new(attr.span(), "#[params] needs `result = Type`"))?;
    let handler = match handler {
        Some(handler) => handler,
        None => {
            let snake = snake_case(&name);
            syn::parse_str::<Ident>(&snake).map_err(|_| {
                Error::new(
                    attr.span(),
                    format!("`{name}` gives no valid fn name; set `handler = \"...\"`"),
                )
            })?
        }
    };
    Ok(Method {
        name,
        handler: format_ident!("{}", handler, span = params.span()),
        params,
        result,
        aliases,
        limit,
    })
}

/// `getShift` → `get_shift`, `tracking.sendLocation` → `tracking_send_location`.
pub(crate) fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut previous_lower = false;
    for ch in name.chars() {
        if ch.is_ascii_uppercase() {
            if previous_lower {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            previous_lower = false;
        } else if ch.is_ascii_alphanumeric() {
            out.push(ch);
            previous_lower = true;
        } else {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            previous_lower = false;
        }
    }
    out.trim_end_matches('_').to_owned()
}

fn has_serde_rename_all(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("serde")
            && attr
                .meta
                .require_list()
                .is_ok_and(|list| list.tokens.to_string().contains("rename_all"))
    })
}

fn item_attrs(item: &Item) -> Option<&Vec<Attribute>> {
    Some(match item {
        Item::Const(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        _ => return None,
    })
}
