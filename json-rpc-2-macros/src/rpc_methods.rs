use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Error, FnArg, ImplItem, ItemImpl, LitStr, Pat, Result, ReturnType, meta::ParseNestedMeta,
    parse::Parser, spanned::Spanned,
};

struct Method {
    ident: syn::Ident,
    params: syn::Type,
    name: String,
    aliases: Vec<String>,
    limit: Option<String>,
}

pub fn expand(args: TokenStream, input: TokenStream) -> Result<TokenStream> {
    let mut alias_prefix = None::<String>;
    syn::meta::parser(|meta| {
        if meta.path.is_ident("alias_prefix") {
            alias_prefix = Some(string_value(&meta)?);
            Ok(())
        } else {
            Err(meta.error("expected `alias_prefix = \"...\"`"))
        }
    })
    .parse2(args)?;

    let mut item: ItemImpl = syn::parse2(input)?;
    if let Some((_, path, _)) = &item.trait_ {
        return Err(Error::new(
            path.span(),
            "#[rpc_methods] expects an inherent impl block",
        ));
    }
    let mut methods = Vec::new();
    for impl_item in &mut item.items {
        let ImplItem::Fn(function) = impl_item else {
            continue;
        };
        let mut found = None;
        let mut error = None::<Error>;
        function.attrs.retain(|attr| {
            if !attr.path().is_ident("rpc") {
                return true;
            }
            let mut name = None;
            let mut aliases = Vec::new();
            let mut limit = None;
            let parsed = if matches!(attr.meta, syn::Meta::Path(_)) {
                Ok(())
            } else {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("name") {
                        name = Some(string_value(&meta)?);
                    } else if meta.path.is_ident("alias") {
                        aliases.push(string_value(&meta)?);
                    } else if meta.path.is_ident("limit") {
                        limit = Some(string_value(&meta)?);
                    } else {
                        return Err(meta.error("expected `name`, `alias` or `limit`"));
                    }
                    Ok(())
                })
            };
            match parsed {
                Ok(()) if found.is_some() => {
                    error = Some(Error::new(attr.span(), "duplicate #[rpc] attribute"));
                }
                Ok(()) => found = Some((name, aliases, limit)),
                Err(err) => error = Some(err),
            }
            false
        });
        if let Some(error) = error {
            return Err(error);
        }
        let Some((name, aliases, limit)) = found else {
            continue;
        };
        let signature = &function.sig;
        if signature.asyncness.is_none() {
            return Err(Error::new(signature.span(), "#[rpc] methods must be async"));
        }
        if matches!(signature.output, ReturnType::Default) {
            return Err(Error::new(
                signature.span(),
                "#[rpc] methods must return RpcResult<T>",
            ));
        }
        let mut inputs = signature.inputs.iter();
        match inputs.next() {
            Some(FnArg::Receiver(receiver))
                if receiver.reference.is_some() && receiver.mutability.is_none() => {}
            _ => {
                return Err(Error::new(
                    signature.span(),
                    "#[rpc] methods must take `&self`",
                ));
            }
        }
        let params = match (inputs.next(), inputs.next()) {
            (Some(FnArg::Typed(arg)), None) if matches!(*arg.pat, Pat::Ident(_)) => {
                (*arg.ty).clone()
            }
            _ => {
                return Err(Error::new(
                    signature.span(),
                    "#[rpc] methods take exactly one params argument after `&self`",
                ));
            }
        };
        methods.push(Method {
            name: name.unwrap_or_else(|| signature.ident.to_string()),
            ident: signature.ident.clone(),
            params,
            aliases,
            limit,
        });
    }
    if methods.is_empty() {
        return Err(Error::new(
            item.self_ty.span(),
            "#[rpc_methods] found no #[rpc] methods",
        ));
    }

    let krate = quote!(::json_rpc_2_server);
    let limited = methods.iter().any(|method| method.limit.is_some());
    let registrations = methods.iter().map(|method| {
        let Method {
            ident,
            params,
            name,
            limit,
            ..
        } = method;
        let handler = quote! {
            |this: ::std::sync::Arc<Self>, params: #params| async move { this.#ident(params).await }
        };
        match limit {
            Some(key) => quote! {
                server.register_limited(#name, ::std::sync::Arc::clone(&self), ::std::sync::Arc::clone(limiter), #key, #handler)?;
            },
            None => quote! {
                server.register_typed(#name, ::std::sync::Arc::clone(&self), #handler)?;
            },
        }
    });
    let aliases = methods.iter().flat_map(|method| {
        let target = &method.name;
        let prefixed = alias_prefix
            .as_ref()
            .map(|prefix| format!("{prefix}{target}"));
        prefixed
            .into_iter()
            .chain(method.aliases.iter().cloned())
            .map(move |alias| quote! { server.register_alias(#alias, #target)?; })
    });
    let limiter_arg = limited.then(|| quote!(, limiter: &::std::sync::Arc<#krate::RateLimiter>));
    let names: Vec<_> = methods
        .iter()
        .map(|method| LitStr::new(&method.name, method.ident.span()))
        .collect();
    let doc = format!(
        "Registers the JSON-RPC methods {} and their aliases; generated by `#[rpc_methods]`.",
        names
            .iter()
            .map(|name| format!("`{}`", name.value()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
    let self_ty = &item.self_ty;
    Ok(quote! {
        #item

        impl #impl_generics #self_ty #ty_generics #where_clause {
            #[doc = #doc]
            pub fn register_rpc(
                self: ::std::sync::Arc<Self>,
                server: &#krate::RpcServer
                #limiter_arg
            ) -> ::std::result::Result<(), #krate::RegistrationError> {
                #(#registrations)*
                #(#aliases)*
                ::std::result::Result::Ok(())
            }
        }
    })
}

pub(crate) fn string_value(meta: &ParseNestedMeta<'_>) -> Result<String> {
    Ok(meta.value()?.parse::<LitStr>()?.value())
}
